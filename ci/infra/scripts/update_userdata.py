#!/usr/bin/env python3
"""Reconcile the EC2 `user_data` of running macOS GitHub Actions runners.

EC2 only accepts a `ModifyInstanceAttribute` for `userData` while the instance
is stopped, and a macOS instance can only run on a Dedicated Host. Stopping a
Mac instance puts its host into a multi-hour scrub (host state `pending`), so
`StartInstances` is rejected with `InsufficientHostCapacity` until the host is
`available` again.

This script encapsulates that whole user_data update workflow so it lives apart
from the praktika `deploy` path:

  1. For each named instance, compare the live `user_data` with the configured
     file; skip the instance when it already matches.
  2. Otherwise stop the instance, install the new `user_data`, and (unless
     `--no-start`) start it again, blocking on the Dedicated Host scrub.

Instances are reconciled one at a time so the whole fleet is never stopped at
once. The whole operation is restricted to the instance ids you pass, so other
instances are left untouched.

Examples:

    update_userdata.py --user-data-file ci/infra/scripts/user_data_macos.txt \\
        --instance i-0123456789abcdef0
    update_userdata.py --user-data-file ci/infra/scripts/user_data_macos.txt \\
        --instance i-0123456789abcdef0 i-0fedcba9876543210 --region us-east-1
"""

import argparse
import base64
import sys
import time

import boto3
from botocore.exceptions import ClientError
from botocore.waiter import WaiterModel, create_waiter_with_client


def user_data_matches(ec2, instance_id, user_data) -> bool:
    """Return True if the instance's live `user_data` already equals `user_data`."""
    resp = ec2.describe_instance_attribute(
        InstanceId=instance_id, Attribute="userData"
    )
    encoded = (resp.get("UserData") or {}).get("Value") or ""
    if isinstance(encoded, bytes):
        encoded = encoded.decode("ascii")
    current = base64.b64decode(encoded).decode("utf-8") if encoded else ""
    return current == user_data


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


def reconcile_instance(ec2, instance_id, user_data, start):
    """Reconcile one instance: skip when the live user_data already matches,
    otherwise stop, reinstall, and (when `start`) start it again.
    """
    if user_data_matches(ec2, instance_id, user_data):
        print(f"{instance_id}: user_data already up to date - skip")
        return

    resp = ec2.describe_instances(InstanceIds=[instance_id])
    instances = [
        inst
        for r in resp.get("Reservations", [])
        for inst in r.get("Instances", [])
    ]
    if not instances:
        raise Exception(f"instance {instance_id} not found")
    state = (instances[0].get("State") or {}).get("Name")

    install_user_data(ec2, instance_id, state, user_data)
    if start:
        start_instances(ec2, [instance_id])


def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--user-data-file",
        required=True,
        help="Path to the user_data script to install (e.g. user_data_macos.txt).",
    )
    parser.add_argument(
        "--instance",
        required=True,
        nargs="+",
        metavar="INSTANCE_ID",
        help=(
            "EC2 instance ids to reconcile (e.g. i-0123456789abcdef0). Each named "
            "instance is stopped, has the configured user_data installed, and is "
            "started again. Other instances are left untouched."
        ),
    )
    parser.add_argument(
        "--region",
        default="us-east-1",
        help="AWS region of the instances (default: us-east-1).",
    )
    parser.add_argument(
        "--no-start",
        action="store_true",
        help="Stop and update user_data but leave the instances stopped.",
    )
    args = parser.parse_args()

    with open(args.user_data_file, "r") as f:
        user_data = f.read()
    print(
        f"loaded user_data from '{args.user_data_file}' ({len(user_data)} bytes)"
    )

    ec2 = boto3.client("ec2", region_name=args.region)
    for instance_id in args.instance:
        reconcile_instance(ec2, instance_id, user_data, start=not args.no_start)


if __name__ == "__main__":
    sys.exit(main())
