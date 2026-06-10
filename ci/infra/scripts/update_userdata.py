#!/usr/bin/env python3
"""Reconcile the EC2 `user_data` of running macOS GitHub Actions runners.

EC2 only accepts a `ModifyInstanceAttribute` for `userData` while the instance
is stopped, and a macOS instance can only run on a Dedicated Host. Stopping a
Mac instance puts its host into a multi-hour scrub (host state `pending`), so
`StartInstances` is rejected with `InsufficientHostCapacity` until the host is
`available` again.

This script encapsulates that whole user_data update workflow so it lives apart
from the praktika `deploy` path. You pass the EC2 instance ids to touch via
`--instance`; the `EC2Instance.Config` entries in `ci/infra/cloud.py` are the
source of truth for everything else. The script discovers each config's live
instances (matching praktika's own tag-based lookup), and for every requested
instance found under a config it uses that config's `region` and
`user_data_file` to:

  1. Compare the live `user_data` with the configured file; skip the instance
     when it already matches.
  2. Otherwise stop the instance, install the new `user_data`, and start it
     again, blocking on the Dedicated Host scrub.

Instances are reconciled one at a time so the whole fleet is never stopped at
once. Only the instance ids you pass are touched; with no `--instance` every
live instance of every config is scanned. By default the script only reports
the plan and prints a diff for the first mismatched instance - pass `--update`
to actually stop, install, and start.

Examples:

    update_userdata.py                                  # report the fleet plan + diff
    update_userdata.py --instance i-0123456789abcdef0   # report one instance
    update_userdata.py --update                         # apply to the whole fleet
    update_userdata.py --instance i-0123456789abcdef0 --update
"""

import argparse
import base64
import difflib
import importlib.util
import os
import sys
import time

import boto3
from botocore.exceptions import ClientError
from botocore.waiter import WaiterModel, create_waiter_with_client

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
CLOUD_CONFIG_PATH = os.path.join(REPO_ROOT, "ci", "infra", "cloud.py")

# Empty `region` in a config means "the default" — match the AWS SDK default
# region used throughout praktika.
DEFAULT_REGION = "us-east-1"


def load_ec2_configs():
    """Load `ci/infra/cloud.py` and return its `EC2Instance.Config`s keyed by name.

    `cloud.py` imports `from praktika import ...` and `from ci.defs.defs import
    ...`, so both the repo root and `ci/` must be importable before it loads.
    """
    for path in (REPO_ROOT, os.path.join(REPO_ROOT, "ci")):
        if path not in sys.path:
            sys.path.insert(0, path)
    spec = importlib.util.spec_from_file_location("cloud", CLOUD_CONFIG_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return {config.name: config for config in module.CLOUD.ec2_instances}


def get_live_user_data(ec2, instance_id) -> str:
    """Return the instance's current `user_data`, decoded to text."""
    resp = ec2.describe_instance_attribute(
        InstanceId=instance_id, Attribute="userData"
    )
    encoded = (resp.get("UserData") or {}).get("Value") or ""
    if isinstance(encoded, bytes):
        encoded = encoded.decode("ascii")
    return base64.b64decode(encoded).decode("utf-8") if encoded else ""


def strip_comments(text) -> str:
    """Drop full-line `#` comments so the match ignores comment-only edits.

    Comments do not affect the booted runner, and pushing a comment-only change
    would cycle the whole Mac fleet through multi-hour Dedicated Host scrubs for
    nothing. The shebang (`#!`) is kept - it is functional. Only lines whose
    first non-blank character is `#` are dropped, so a `#` inside a string or
    heredoc is left untouched.
    """
    return "\n".join(
        line
        for line in text.splitlines()
        if not line.lstrip().startswith("#") or line.lstrip().startswith("#!")
    )


def install_user_data(ec2, instance_id, state, user_data):
    """Stop the instance (waiting until fully stopped) and install `user_data` via
    `ModifyInstanceAttribute`, which requires the instance to be stopped (otherwise
    it returns `IncorrectInstanceState`).
    """
    if state in ("pending", "running"):
        print(f"stopping {instance_id} to update user_data")
        ec2.stop_instances(InstanceIds=[instance_id])
    if state != "stopped":
        # A Mac instance stop can exceed the waiter's 10-min default (15s x 40)
        # once the host starts scrubbing, so wait up to 40 min.
        ec2.get_waiter("instance_stopped").wait(
            InstanceIds=[instance_id],
            WaiterConfig={"Delay": 30, "MaxAttempts": 80},
        )

    # UserData.Value is a blob; boto3 base64-encodes it for the API call, so pass
    # the raw script bytes here. Pre-encoding would double-encode and store base64
    # text as the boot script.
    ec2.modify_instance_attribute(
        InstanceId=instance_id,
        UserData={"Value": user_data.encode("utf-8")},
    )
    print(f"updated user_data on {instance_id}")


def wait_for_hosts_available(ec2, instance_ids, require_all=True):
    """Block until the Dedicated Host(s) backing `instance_ids` finish the EC2 Mac
    scrub workflow and become `available`. boto3 has no built-in host waiter, so
    define one over `DescribeHosts` (poll every 60s for up to 3h, the worst-case
    Mac scrub window). With `require_all=False` it returns as soon as any one of
    the hosts is available.
    """
    resp = ec2.describe_instances(InstanceIds=instance_ids)
    host_ids = sorted(
        {
            inst.get("Placement", {}).get("HostId")
            for r in resp.get("Reservations", [])
            for inst in r.get("Instances", [])
            if inst.get("Placement", {}).get("HostId")
        }
    )
    if not host_ids:
        raise Exception(
            f"cannot wait for host capacity - no dedicated host associated with "
            f"{instance_ids}"
        )

    print(
        f"waiting for dedicated host(s) {host_ids} to finish scrubbing; "
        f"press Ctrl+C to abort waiting"
    )
    model = WaiterModel(
        {
            "version": 2,
            "waiters": {
                "HostAvailable": {
                    "operation": "DescribeHosts",
                    "delay": 60,
                    "maxAttempts": 180,
                    "acceptors": [
                        {
                            "state": "success",
                            "matcher": "pathAll" if require_all else "pathAny",
                            "argument": "Hosts[].State",
                            "expected": "available",
                        },
                        {
                            "state": "failure",
                            "matcher": "pathAny",
                            "argument": "Hosts[].State",
                            "expected": "permanent-failure",
                        },
                    ],
                }
            },
        }
    )
    waiter = create_waiter_with_client("HostAvailable", model, ec2)
    waiter.wait(HostIds=host_ids)


def start_instances(ec2, instance_ids):
    """Start the given (stopped) instances, each as soon as its EC2 Mac dedicated
    host finishes scrubbing. We try every instance up front, then wait for a
    blocked host to recover and retry - so a ready instance never waits behind a
    still-scrubbing one.
    """
    pending = [i for i in instance_ids if i]
    if not pending:
        return

    started = []
    while pending:
        blocked = []
        for instance_id in pending:
            try:
                ec2.start_instances(InstanceIds=[instance_id])
            except ClientError as e:
                if (
                    e.response.get("Error", {}).get("Code")
                    == "InsufficientHostCapacity"
                ):
                    blocked.append(instance_id)
                    continue
                raise
            print(f"starting {instance_id}")
            started.append(instance_id)

        if not blocked:
            break

        # Wait until at least one blocked host becomes available, then retry all
        # blocked instances (the recovered one(s) start, the rest loop).
        wait_for_hosts_available(ec2, blocked, require_all=False)
        # A host can report `available` a moment before StartInstances will accept
        # it; sleep briefly so a transient rejection paces the retry instead of
        # spinning the loop.
        time.sleep(10)
        pending = blocked

    ec2.get_waiter("instance_running").wait(InstanceIds=started)


def reconcile_instance(ec2, instance_id, state, user_data, update, show_diff):
    """Reconcile one instance: skip when the live user_data already matches,
    otherwise (when `update`) stop, reinstall, and start it again.

    Without `update`, make no changes - only report. When `show_diff` and the
    user_data differs, print a unified diff of live vs configured first. Returns
    True if a diff was printed, so the caller can show it only for the first
    mismatch.

    Comments are ignored when matching (see `strip_comments`), so a comment-only
    edit does not trigger a fleet-wide stop/start.
    """
    live = get_live_user_data(ec2, instance_id)
    if strip_comments(live) == strip_comments(user_data):
        print(f"{instance_id}: user_data already up to date - skip")
        return False

    printed = False
    if show_diff:
        print(f"{instance_id}: user_data differs (state={state}):")
        sys.stdout.writelines(
            difflib.unified_diff(
                live.splitlines(keepends=True),
                user_data.splitlines(keepends=True),
                fromfile=f"{instance_id} (live)",
                tofile="configured",
            )
        )
        if not live.endswith("\n") or not user_data.endswith("\n"):
            print()  # keep the next log line on its own row
        printed = True

    if not update:
        print(
            f"{instance_id}: would update (state={state}) - pass --update to apply"
        )
        return printed

    install_user_data(ec2, instance_id, state, user_data)
    start_instances(ec2, [instance_id])
    return printed


def load_user_data(config):
    """Read the `user_data` file configured for `config`, resolving a relative
    path against the repo root.
    """
    user_data_file = config.user_data_file
    if not user_data_file:
        raise Exception(f"config '{config.name}' has no user_data_file")
    if not os.path.isabs(user_data_file):
        user_data_file = os.path.join(REPO_ROOT, user_data_file)
    with open(user_data_file, "r") as f:
        user_data = f.read()
    print(
        f"[{config.name}] region={config.region}, user_data='{user_data_file}' "
        f"({len(user_data)} bytes)"
    )
    return user_data


def reconcile_instances(configs, requested_ids, update=False):
    """Reconcile instances, sourcing each one's `region` and `user_data_file`
    from the `ci/infra/cloud.py` config that owns it.

    With `requested_ids` set, only those ids are touched and an id that belongs
    to no config is an error. With `requested_ids` None, every live instance of
    every config is scanned and reconciled.

    Each config's live instances are discovered with praktika's own tag-based
    lookup (`_find_existing_instances`), so the standalone update matches what
    praktika `deploy` would launch. Without `update`, report the plan and print
    a diff for the first mismatched instance without changing anything.
    """
    pending = None if requested_ids is None else set(requested_ids)
    diff_printed = False
    for config in configs.values():
        if pending is not None and not pending:
            break
        # `_find_existing_instances` builds its own boto3 client from
        # `config.region`, so pin the default region on the config first.
        config.region = config.region or DEFAULT_REGION

        matched = [
            inst
            for inst in config._find_existing_instances()
            if pending is None or inst.get("InstanceId") in pending
        ]
        if not matched:
            continue

        user_data = load_user_data(config)
        ec2 = boto3.client("ec2", region_name=config.region)
        for inst in matched:
            instance_id = inst.get("InstanceId")
            state = (inst.get("State") or {}).get("Name")
            # Show the diff only for the first mismatched instance - the
            # configured user_data is the same for every instance of a config.
            diff_printed |= reconcile_instance(
                ec2,
                instance_id,
                state,
                user_data,
                update=update,
                show_diff=not diff_printed,
            )
            if pending is not None:
                pending.discard(instance_id)

    if pending:
        raise Exception(
            f"instance(s) {sorted(pending)} not found under any EC2Instance "
            f"config in ci/infra/cloud.py"
        )


def main():
    configs = load_ec2_configs()

    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--instance",
        nargs="+",
        metavar="INSTANCE_ID",
        help=(
            "EC2 instance ids to reconcile (e.g. i-0123456789abcdef0). Each id "
            "is matched to its ci/infra/cloud.py config to source the region "
            "and user_data file, then the instance is stopped, has the "
            "configured user_data installed, and is started again. Other "
            "instances are left untouched. When omitted, every live instance of "
            "every config is scanned and reconciled."
        ),
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help=(
            "Actually perform the update (stop, install user_data, start). "
            "Without it the script only reports the plan and prints a diff for "
            "the first mismatched instance, making no changes."
        ),
    )
    args = parser.parse_args()

    if not args.update:
        print("report only - pass --update to apply changes")
    reconcile_instances(configs, args.instance, update=args.update)


if __name__ == "__main__":
    sys.exit(main())
