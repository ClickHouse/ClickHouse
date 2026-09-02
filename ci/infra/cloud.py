from praktika import CloudInfrastructure
from praktika.infrastructure.dedicated_host import DedicatedHost
from praktika.infrastructure.ec2_instance import EC2Instance

from ci.defs.defs import RunnerLabels

MAC_OS_TAHOE_IMAGE_AMI = "ami-0f8ce53a93ab42329"
MAC_OS_TAHOE_ARM_IMAGE_AMI = "ami-0cbd6d494543c15b3"

MAC_VPC_NAME = "ci-cd"

EC2_INSTANCE_PROFILE_NAME = "untrusted_runner"

MACOS_ARM_SMALL_RUNNER_LABELS = [
    RunnerLabels.MACOS_ARM_SMALL[1],
    f"pr-{RunnerLabels.MACOS_ARM_SMALL[1]}",
]


CLOUD = CloudInfrastructure.Config(
    name="cloud_ci_infra",
    lambda_functions=[*CloudInfrastructure.SLACK_APP_LAMBDAS],
    dedicated_hosts=[
        DedicatedHost.Config(
            name="mac2-m2pro.metal",
            availability_zones=[
                "ap-southeast-2b",
            ],
            instance_type="mac2-m2pro.metal",
            auto_placement="on",
            # 10 of the 16 hosts allowed by quota `L-14F120D1`, all managed by
            # praktika.
            quantity_per_az=10,
            praktika_resource_tag="mac_m2_pro",
        ),
    ],
    ec2_instances=[
        EC2Instance.Config(
            name=RunnerLabels.MACOS_ARM_SMALL[1],
            image_id=MAC_OS_TAHOE_ARM_IMAGE_AMI,
            instance_type="mac2-m2pro.metal",
            region="ap-southeast-2",
            subnet_id="subnet-09516f5db7b5bfac2",
            security_group_ids=["sg-0f2c7852169121ec5"],
            iam_instance_profile_name=EC2_INSTANCE_PROFILE_NAME,
            key_name="awswork",
            user_data_file="./ci/infra/scripts/user_data_macos.txt",
            root_volume_type="gp3",
            root_volume_size=100,
            root_volume_encrypted=True,
            tenancy="host",
            praktika_resource_tag="mac_m2_pro",
            runner_labels=MACOS_ARM_SMALL_RUNNER_LABELS,
            # One instance per host across all 10 managed hosts. A single
            # instance completes about three fast test runs per hour, so 10 of
            # them absorb the ~18 runs per hour that were queueing up behind 6
            # instances.
            quantity=10,
        ),
    ]
)
