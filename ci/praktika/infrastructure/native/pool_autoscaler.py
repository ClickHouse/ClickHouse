import json
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List

from praktika.infrastructure.iam_role import IAMRole
from praktika.infrastructure.lambda_function import Lambda

from . import iam_scope


def _rate_expression_for_seconds(interval_seconds: int) -> str:
    interval_seconds = max(60, int(interval_seconds))
    minutes = (interval_seconds + 59) // 60
    unit = "minute" if minutes == 1 else "minutes"
    return f"rate({minutes} {unit})"


@dataclass
class PoolAutoscaler:
    @dataclass
    class Pool:
        name: str
        queue_name: str = ""
        asg_name: str = ""
        capacity_reserve: int = 0
        ext: Dict[str, Any] = field(default_factory=dict)

    name: str = "pool-autoscaler"
    interval_seconds: int = 60
    pools: List[Pool] = field(default_factory=list)
    lambda_role_name: str = "pool-autoscaler-role"
    timeout_ms: int = 30 * 1000
    memory_size_mb: int = 128
    ext: Dict[str, Any] = field(default_factory=dict)

    lambda_config: Lambda.Config = field(init=False)
    lambda_role: IAMRole.Config = field(init=False)

    def __post_init__(self):
        if not self.pools:
            raise ValueError("PoolAutoscaler requires at least one pool")
        for pool in self.pools:
            pool.capacity_reserve = int(pool.capacity_reserve)
            if pool.capacity_reserve < 0:
                raise ValueError(
                    f"PoolAutoscaler pool {pool.name!r} has negative capacity_reserve"
                )

        pool_config_json = json.dumps(
            [
                {
                    "name": pool.name,
                    "queue_name": pool.queue_name,
                    "asg_name": pool.asg_name,
                    "capacity_reserve": pool.capacity_reserve,
                }
                for pool in self.pools
            ],
            separators=(",", ":"),
            sort_keys=True,
        )
        queue_arns = sorted(
            {
                f"arn:aws:sqs:*:*:{pool.queue_name or pool.name}"
                for pool in self.pools
                if (pool.queue_name or pool.name)
            }
        )

        self.lambda_role = IAMRole.Config(
            name=self.lambda_role_name,
            trust_service="lambda.amazonaws.com",
            policy_arns=[
                "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
            ],
            inline_policies={
                "PoolAutoscalerAccess": {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            # Describe does not support resource-level scoping.
                            "Effect": "Allow",
                            "Action": ["autoscaling:DescribeAutoScalingGroups"],
                            "Resource": "*",
                        },
                        {
                            # Scale-out is limited to this project's ASGs.
                            "Effect": "Allow",
                            "Action": ["autoscaling:UpdateAutoScalingGroup"],
                            "Resource": iam_scope.autoscaling_group_arns(),
                        },
                        {
                            "Effect": "Allow",
                            "Action": [
                                "sqs:GetQueueUrl",
                                "sqs:GetQueueAttributes",
                            ],
                            "Resource": queue_arns,
                        },
                    ],
                }
            },
        )
        self.lambda_config = Lambda.Config(
            name=self.name,
            path=os.path.join(os.path.dirname(__file__), "lambda_pool_autoscaler.py"),
            handler="lambda_pool_autoscaler.lambda_handler",
            role_name=self.lambda_role_name,
            environments={
                "POOLS_CONFIG_JSON": pool_config_json,
                "POLL_INTERVAL_SECONDS": str(max(60, int(self.interval_seconds))),
            },
            timeout_ms=self.timeout_ms,
            memory_size_mb=self.memory_size_mb,
            schedule_expression=_rate_expression_for_seconds(self.interval_seconds),
        )

    @classmethod
    def from_pools(
        cls,
        pools,
        *,
        name: str = "pool-autoscaler",
        interval_seconds: int = 60,
        lambda_role_name: str = "pool-autoscaler-role",
        timeout_ms: int = 30 * 1000,
        memory_size_mb: int = 128,
    ):
        autoscaled_pools = [
            cls.Pool(
                name=getattr(pool, "name", ""),
                queue_name=getattr(getattr(pool, "queue", None), "name", ""),
                asg_name=getattr(getattr(pool, "autoscaling_group", None), "name", ""),
                capacity_reserve=getattr(pool, "capacity_reserve", 0),
            )
            for pool in pools
            if getattr(pool, "scaling", "") == "auto"
        ]
        if not autoscaled_pools:
            return None
        return cls(
            name=name,
            interval_seconds=interval_seconds,
            pools=autoscaled_pools,
            lambda_role_name=lambda_role_name,
            timeout_ms=timeout_ms,
            memory_size_mb=memory_size_mb,
        )
