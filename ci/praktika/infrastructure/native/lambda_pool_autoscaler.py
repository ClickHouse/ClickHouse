import json
import math
import os

import boto3


def _load_pool_configs():
    raw = os.environ.get("POOLS_CONFIG_JSON", "[]").strip()
    if not raw:
        return []
    payload = json.loads(raw)
    if not isinstance(payload, list):
        raise ValueError("POOLS_CONFIG_JSON must contain a JSON array")
    return payload


def _calculate_desired_capacity(
    *,
    current_desired: int,
    max_size: int,
    visible_messages: int,
    in_flight_messages: int,
    capacity_reserve: int = 0,
) -> int:
    # Scheduled autoscaling is scale-up only. Scale-in is owned by the VMs
    # themselves after they observe sustained queue idleness from inside the
    # worker/orchestrator loop. Never return a lower desired capacity here,
    # even if backlog is currently zero.
    backlog = max(0, int(visible_messages)) + max(0, int(in_flight_messages))
    capacity_reserve = max(0, int(capacity_reserve))
    current_work_capacity = max(0, int(current_desired) - capacity_reserve)
    required_work_capacity = math.ceil(backlog / 1) if backlog else 0
    target_work_capacity = max(current_work_capacity, required_work_capacity)
    if target_work_capacity <= current_work_capacity:
        return min(max_size, max(current_desired, capacity_reserve))

    new_work_capacity = min(target_work_capacity, current_work_capacity + 1)
    target_capacity = min(max_size, capacity_reserve + new_work_capacity)
    if target_capacity <= current_desired:
        return current_desired
    return max(current_desired, target_capacity)


def lambda_handler(event, context):
    region = (
        os.environ.get("AWS_REGION")
        or os.environ.get("AWS_DEFAULT_REGION")
        or "us-east-1"
    )
    sqs = boto3.client("sqs", region_name=region)
    autoscaling = boto3.client("autoscaling", region_name=region)

    results = []
    for pool in _load_pool_configs():
        pool_name = str(pool["name"])
        queue_name = str(pool.get("queue_name") or pool_name)
        asg_name = str(pool.get("asg_name") or pool_name)
        capacity_reserve = max(0, int(pool.get("capacity_reserve") or 0))

        queue_url = sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]
        queue_attrs = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=[
                "ApproximateNumberOfMessages",
                "ApproximateNumberOfMessagesNotVisible",
            ],
        )["Attributes"]
        visible_messages = int(queue_attrs.get("ApproximateNumberOfMessages", "0"))
        in_flight_messages = int(
            queue_attrs.get("ApproximateNumberOfMessagesNotVisible", "0")
        )

        group = autoscaling.describe_auto_scaling_groups(
            AutoScalingGroupNames=[asg_name]
        )["AutoScalingGroups"]
        if not group:
            raise RuntimeError(f"Auto Scaling Group '{asg_name}' was not found")
        group = group[0]
        current_desired = int(group["DesiredCapacity"])
        max_size = int(group["MaxSize"])

        proposed_desired = _calculate_desired_capacity(
            current_desired=current_desired,
            max_size=max_size,
            visible_messages=visible_messages,
            in_flight_messages=in_flight_messages,
            capacity_reserve=capacity_reserve,
        )
        # Defensive clamp: even if future _calculate_desired_capacity changes,
        # this autoscaler must never scale a pool down. VM self-termination is
        # the only supported scale-in path.
        new_desired = max(current_desired, proposed_desired)
        scaled = new_desired > current_desired
        if scaled:
            autoscaling.update_auto_scaling_group(
                AutoScalingGroupName=asg_name,
                DesiredCapacity=new_desired,
            )

        results.append(
            {
                "pool_name": pool_name,
                "asg_name": asg_name,
                "queue_name": queue_name,
                "visible_messages": visible_messages,
                "in_flight_messages": in_flight_messages,
                "current_desired": current_desired,
                "capacity_reserve": capacity_reserve,
                "new_desired": new_desired,
                "scaled": scaled,
            }
        )

    return {
        "region": region,
        "pool_count": len(results),
        "results": results,
    }
