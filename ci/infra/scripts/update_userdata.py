#!/usr/bin/env python3
"""Reconcile the EC2 `user_data` of running macOS GitHub Actions runners.

EC2 only accepts a `ModifyInstanceAttribute` for `userData` while the instance
is stopped, and a macOS instance can only run on a Dedicated Host. Stopping a
Mac instance puts its host into a multi-hour scrub (host state `pending`), so
`StartInstances` is rejected with `InsufficientHostCapacity` until the host is
`available` again.

This script encapsulates that whole user_data update workflow so it lives apart
from the praktika `deploy` path. It runs in two phases:

  1. `build_plan` reads the live state (read-only) and produces a list of
     `PlanItem`s. An instance gets an `update` item when its live user_data
     differs (stop, install, start) or a `start` item when its user_data is
     current but it is stopped. A running, up-to-date instance needs no action
     and is left out of the plan (logged as skipped).
  2. Applying loops the items, one at a time, blocking on the Dedicated Host
     scrub. Already-stopped instances are started first, before any update -
     an update stops its instance and triggers a host scrub that would block
     those starts.

The plan is always printed. By default that is all that happens - pass
`--apply` to execute it.

The `EC2Instance.Config` entries in `ci/infra/cloud.py` are the source of truth:
each config supplies the `region` and `user_data_file`, and its live instances
are discovered with praktika's own tag-based lookup. You pass the EC2 instance
ids to touch via `--instance`; with no `--instance` every live instance of every
config is scanned. Comments are ignored when matching user_data, so a
comment-only edit plans no update.

Examples:

    update_userdata.py                                  # print the fleet plan
    update_userdata.py --instance i-0123456789abcdef0   # plan one instance
    update_userdata.py --apply                          # apply to the whole fleet
    update_userdata.py --instance i-0123456789abcdef0 --apply
"""

import argparse
import base64
import difflib
import importlib.util
import os
import sys
import time
from dataclasses import dataclass
from typing import List

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
    """Drop every line whose first non-blank character is `#` so the match
    ignores comment-only edits. The shebang (`#!`) is kept - it is functional.

    Comments do not affect the booted runner, and pushing a comment-only change
    would cycle the whole Mac fleet through multi-hour Dedicated Host scrubs for
    nothing.

    This is a line-level strip, not a shell parser: a `#`-leading line inside a
    heredoc or quoted string would also be dropped, which would wrongly equate
    two functionally different scripts. That is acceptable here because the
    `user_data` bootstrap is a flat script with no such lines; do not reuse this
    on arbitrary scripts without revisiting that assumption.
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


# Plan item actions. Running instances with current user_data need no action
# and are not added to the plan at all.
UPDATE = "update"  # live user_data differs - stop, install, then start
START = "start"  # user_data current but instance stopped - just start


@dataclass
class PlanItem:
    """One planned action for one instance, self-contained enough to `apply`.

    `build_plan` fills these in read-only; `apply` carries a single item out,
    waiting out the Dedicated Host scrub as part of the start.
    """

    config: str
    region: str
    instance_id: str
    state: str
    action: str
    user_data: str  # configured content, installed on UPDATE
    live: str  # live content, for the diff on UPDATE

    def describe(self, show_diff):
        """Print this item's plan line. On an UPDATE, when `show_diff`, print a
        unified diff of live vs configured first; return True if it did, so the
        caller shows the diff only for the first mismatch.
        """
        if self.action == START:
            print(
                f"  {self.instance_id} [{self.config}] {self.state}: "
                f"start (wait for host scrub, then start)"
            )
            return False

        printed = False
        if show_diff:
            print(f"  {self.instance_id} [{self.config}] user_data diff (live -> configured):")
            sys.stdout.writelines(
                difflib.unified_diff(
                    self.live.splitlines(keepends=True),
                    self.user_data.splitlines(keepends=True),
                    fromfile=f"{self.instance_id} (live)",
                    tofile="configured",
                )
            )
            if not self.live.endswith("\n") or not self.user_data.endswith("\n"):
                print()  # keep the next log line on its own row
            printed = True
        print(
            f"  {self.instance_id} [{self.config}] {self.state}: "
            f"update (stop, install, wait for host scrub, then start)"
        )
        return printed

    def apply(self):
        """Carry out this item. For an UPDATE: stop, install user_data, then
        start. For a START: just start a stopped instance. Either way the start
        blocks on the Dedicated Host scrub (see `start_instances`).
        """
        ec2 = boto3.client("ec2", region_name=self.region)
        if self.action == UPDATE:
            install_user_data(ec2, self.instance_id, self.state, self.user_data)
        elif self.action == START and self.state == "stopping":
            # `start_instances` needs a fully stopped instance.
            ec2.get_waiter("instance_stopped").wait(
                InstanceIds=[self.instance_id],
                WaiterConfig={"Delay": 30, "MaxAttempts": 80},
            )
        start_instances(ec2, [self.instance_id])


def print_plan(plan):
    """Print every item in `plan`, with a diff for the first instance that needs
    an update.
    """
    diff_shown = False
    for item in plan:
        diff_shown = item.describe(show_diff=not diff_shown) or diff_shown


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
        return f.read()


def build_plan(configs, requested_ids) -> List[PlanItem]:
    """Read live state (read-only) and return the list of `PlanItem`s.

    Each one's `region` and `user_data_file` come from the `ci/infra/cloud.py`
    config that owns it. With `requested_ids` set, only those ids are planned
    and an id that belongs to no config is an error. With `requested_ids` None,
    every live instance of every config is planned.

    Each config's live instances are discovered with praktika's own tag-based
    lookup (`_find_existing_instances`), so the plan matches what praktika
    `deploy` would launch. Comments are ignored when matching user_data (see
    `strip_comments`), so a comment-only edit plans no update.
    """
    pending = None if requested_ids is None else set(requested_ids)
    starts: List[PlanItem] = []
    updates: List[PlanItem] = []
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
            live = get_live_user_data(ec2, instance_id)
            item = PlanItem(
                config=config.name,
                region=config.region,
                instance_id=instance_id,
                state=state,
                action="",
                user_data=user_data,
                live=live,
            )
            if strip_comments(live) != strip_comments(user_data):
                item.action = UPDATE
                updates.append(item)
            elif state in ("stopped", "stopping"):
                item.action = START
                starts.append(item)
            else:
                # Running with current user_data - nothing to do; not planned.
                print(f"  {instance_id} [{config.name}] {state}: up to date - skip")
            if pending is not None:
                pending.discard(instance_id)

    if pending:
        raise Exception(
            f"instance(s) {sorted(pending)} not found under any EC2Instance "
            f"config in ci/infra/cloud.py"
        )

    # Start already-stopped instances before any update: an update stops its
    # instance, which puts the Dedicated Host into a multi-hour scrub that would
    # then block these starts.
    return starts + updates


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
            "EC2 instance ids to plan (e.g. i-0123456789abcdef0). Each id is "
            "matched to its ci/infra/cloud.py config to source the region and "
            "user_data file. When omitted, every live instance of every config "
            "is planned."
        ),
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help=(
            "Execute the plan (stop/install/start updates, start stopped "
            "instances). Without it the plan is only printed, making no changes."
        ),
    )
    args = parser.parse_args()

    plan = build_plan(configs, args.instance)
    print_plan(plan)

    if not plan:
        print("nothing to do - every instance is up to date and running")
        return
    if not args.apply:
        print(f"{len(plan)} change(s) planned - pass --apply to execute")
        return

    print(f"applying {len(plan)} change(s)...")
    for item in plan:
        item.apply()


if __name__ == "__main__":
    sys.exit(main())
