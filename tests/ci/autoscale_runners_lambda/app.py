#!/usr/bin/env python3

"""The lambda to decrease/increase ASG desired capacity based on current queue"""

import logging
from dataclasses import dataclass
from pprint import pformat
from typing import Any, List, Literal, Optional, Tuple

import boto3  # type: ignore
from lambda_shared import (
    RUNNER_TYPE_LABELS,
    CHException,
    ClickHouseHelper,
    get_parameter_from_ssm,
)

### Update comment on the change ###
# 4 HOUR - is a balance to get the most precise values
#   - Our longest possible running check is around 5h on the worst scenario
#   - The long queue won't be wiped out and replaced, so the measurmenet is fine
#   - If the data is spoiled by something, we are from the bills perspective
# Changed it to 3 HOUR: in average we have 1h tasks, but p90 is around 2h.
# With 4h we have too much wasted computing time in case of issues with DB
QUEUE_QUERY = f"""SELECT
    last_status AS status,
    toUInt32(count()) AS length,
    labels
FROM
(
    SELECT
        arraySort(groupArray(status))[-1] AS last_status,
        labels,
        id,
        html_url
    FROM default.workflow_jobs
    WHERE has(labels, 'self-hosted')
        AND hasAny({RUNNER_TYPE_LABELS}, labels)
        AND started_at > now() - INTERVAL 3 HOUR
    GROUP BY ALL
    HAVING last_status IN ('in_progress', 'queued')
)
GROUP BY ALL
ORDER BY labels, last_status"""


@dataclass
class Queue:
    status: Literal["in_progress", "queued"]
    lentgh: int
    label: str


def get_scales(runner_type: str) -> Tuple[int, int]:
    "returns the multipliers for scaling down and up ASG by types"
    # Scaling down is quicker on the lack of running jobs than scaling up on
    # queue

    # The ASG should deflate almost instantly
    scale_down = 1
    # the style checkers have so many noise, so it scales up too quickly
    # The 5 was too quick, there are complainings regarding too slow with
    # 10. I am trying 7 now.
    # 7 still looks a bit slow, so I try 6
    # Let's have it the same as the other ASG
    #
    # All type of style-checkers should be added very quickly to not block the workflows
    # UPDATE THE COMMENT ON CHANGES
    scale_up = 3
    if "style" in runner_type:
        scale_up = 1
    return scale_down, scale_up


CH_CLIENT = None  # type: Optional[ClickHouseHelper]


def set_capacity(
    runner_type: str, queues: List[Queue], client: Any, dry_run: bool = True
) -> None:
    assert len(queues) in (1, 2)
    assert all(q.label == runner_type for q in queues)
    as_groups = client.describe_auto_scaling_groups(
        Filters=[
            {"Name": "tag-key", "Values": ["github:runner-type"]},
            {"Name": "tag-value", "Values": [runner_type]},
        ]
    )["AutoScalingGroups"]
    assert len(as_groups) == 1
    asg = as_groups[0]
    running = 0
    queued = 0
    for q in queues:
        if q.status == "in_progress":
            running = q.lentgh
            continue
        if q.status == "queued":
            queued = q.lentgh
            continue
        raise ValueError("Queue status is not in ['in_progress', 'queued']")

    scale_down, scale_up = get_scales(runner_type)
    # With lyfecycle hooks some instances are actually free because some of
    # them are in 'Terminating:Wait' state
    effective_capacity = max(
        asg["DesiredCapacity"],
        len([ins for ins in asg["Instances"] if ins["HealthStatus"] == "Healthy"]),
    )

    # How much nodes are free (positive) or need to be added (negative)
    capacity_reserve = effective_capacity - running - queued
    stop = False
    if capacity_reserve < 0:
        # This part is about scaling up
        capacity_deficit = -capacity_reserve
        # It looks that we are still OK, since no queued jobs exist
        stop = stop or queued == 0
        # Are we already at the capacity limits
        stop = stop or asg["MaxSize"] <= asg["DesiredCapacity"]
        # Let's calculate a new desired capacity
        # (capacity_deficit + scale_up - 1) // scale_up : will increase min by 1
        # if there is any capacity_deficit
        new_capacity = (
            asg["DesiredCapacity"] + (capacity_deficit + scale_up - 1) // scale_up
        )
        new_capacity = max(new_capacity, asg["MinSize"])
        new_capacity = min(new_capacity, asg["MaxSize"])
        # Finally, should the capacity be even changed
        stop = stop or asg["DesiredCapacity"] == new_capacity
        if stop:
            logging.info(
                "Do not increase ASG %s capacity, current capacity=%s, effective "
                "capacity=%s, maximum capacity=%s, running jobs=%s, queue size=%s",
                asg["AutoScalingGroupName"],
                asg["DesiredCapacity"],
                effective_capacity,
                asg["MaxSize"],
                running,
                queued,
            )
            return

        logging.info(
            "The ASG %s capacity will be increased to %s, current capacity=%s, "
            "effective capacity=%s, maximum capacity=%s, running jobs=%s, queue size=%s",
            asg["AutoScalingGroupName"],
            new_capacity,
            asg["DesiredCapacity"],
            effective_capacity,
            asg["MaxSize"],
            running,
            queued,
        )
        if not dry_run:
            client.set_desired_capacity(
                AutoScalingGroupName=asg["AutoScalingGroupName"],
                DesiredCapacity=new_capacity,
            )
        return

    # Now we will calculate if we need to scale down
    stop = stop or asg["DesiredCapacity"] == asg["MinSize"]
    new_capacity = asg["DesiredCapacity"] - (capacity_reserve // scale_down)
    new_capacity = max(new_capacity, asg["MinSize"])
    new_capacity = min(new_capacity, asg["MaxSize"])
    stop = stop or asg["DesiredCapacity"] == new_capacity
    if stop:
        logging.info(
            "Do not decrease ASG %s capacity, current capacity=%s, effective "
            "capacity=%s, minimum capacity=%s, running jobs=%s, queue size=%s",
            asg["AutoScalingGroupName"],
            asg["DesiredCapacity"],
            effective_capacity,
            asg["MinSize"],
            running,
            queued,
        )
        return

    logging.info(
        "The ASG %s capacity will be decreased to %s, current capacity=%s, effective "
        "capacity=%s, minimum capacity=%s, running jobs=%s, queue size=%s",
        asg["AutoScalingGroupName"],
        new_capacity,
        asg["DesiredCapacity"],
        effective_capacity,
        asg["MinSize"],
        running,
        queued,
    )
    if not dry_run:
        client.set_desired_capacity(
            AutoScalingGroupName=asg["AutoScalingGroupName"],
            DesiredCapacity=new_capacity,
        )


def main(dry_run: bool = True) -> None:
    logging.getLogger().setLevel(logging.INFO)
    asg_client = boto3.client("autoscaling")
    try:
        global CH_CLIENT
        CH_CLIENT = CH_CLIENT or ClickHouseHelper(
            get_parameter_from_ssm("clickhouse-test-stat-url"), "play"
        )
        queues = CH_CLIENT.select_json_each_row("default", QUEUE_QUERY)
    except CHException as ex:
        logging.exception(
            "Got an exception on insert, tryuing to update the client "
            "credentials and repeat",
            exc_info=ex,
        )
        CH_CLIENT = ClickHouseHelper(
            get_parameter_from_ssm("clickhouse-test-stat-url"), "play"
        )
        queues = CH_CLIENT.select_json_each_row("default", QUEUE_QUERY)

    logging.info("Received queue data:\n%s", pformat(queues, width=120))
    for runner_type in RUNNER_TYPE_LABELS:
        runner_queues = [
            Queue(queue["status"], queue["length"], runner_type)
            for queue in queues
            if runner_type in queue["labels"]
        ]
        runner_queues = runner_queues or [Queue("in_progress", 0, runner_type)]
        set_capacity(runner_type, runner_queues, asg_client, dry_run)


def handler(event: dict, context: Any) -> None:
    _ = event
    _ = context
    return main(False)
