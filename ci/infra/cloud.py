from praktika import CloudInfrastructure
from praktika.infrastructure.dedicated_host import DedicatedHost
from praktika.infrastructure.ec2_instance import EC2Instance

from ci.defs.defs import RunnerLabels

MAC_OS_TAHOE_IMAGE_AMI = "ami-0f8ce53a93ab42329"
MAC_OS_SEQUOIA_AMD_IMAGE_AMI = "ami-0d66088cfc49c54b0"
MAC_OS_TAHOE_ARM_IMAGE_AMI = "ami-0cbd6d494543c15b3"

MAC_VPC_NAME = "ci-cd"
MAC_SECURITY_GROUP_IDS = ["sg-061fd9184274476c0"]

EC2_INSTANCE_PROFILE_NAME = "untrusted_runner"

MACOS_AMD_SMALL_RUNNER_LABELS = [
    RunnerLabels.MACOS_AMD_SMALL[1],
    f"pr-{RunnerLabels.MACOS_AMD_SMALL[1]}",
]
MACOS_ARM_SMALL_RUNNER_LABELS = [
    RunnerLabels.MACOS_ARM_SMALL[1],
    f"pr-{RunnerLabels.MACOS_ARM_SMALL[1]}",
]


CLOUD = CloudInfrastructure.Config(
    name="cloud_ci_infra",
    lambda_functions=[*CloudInfrastructure.SLACK_APP_LAMBDAS],
    dedicated_hosts=[
        DedicatedHost.Config(
            name="mac1.metal",
            availability_zones=[
                "us-east-1a",
            ],
            instance_type="mac1.metal",
            auto_placement="on",
            quantity_per_az=2,
            praktika_resource_tag="mac",
        ),
        DedicatedHost.Config(
            name="mac2-m2pro.metal",
            availability_zones=[
                "ap-southeast-2b",
            ],
            instance_type="mac2-m2pro.metal",
            auto_placement="on",
            # 10 of the 16 hosts allowed by quota `L-14F120D1`. One of them was
            # allocated outside praktika and carries the name
            # `clickhouse-pr-macos-m2-e753fc`; it is counted here because it
            # occupies a slot under the quota, but it runs no managed instance.
            quantity_per_az=10,
            praktika_resource_tag="mac_m2_pro",
        ),
    ],
    ec2_instances=[
        EC2Instance.Config(
            name=RunnerLabels.MACOS_AMD_SMALL[1],
            image_id=MAC_OS_SEQUOIA_AMD_IMAGE_AMI,
            instance_type="mac1.metal",
            subnet_id="subnet-0a3886c4db842da5b",
            security_group_ids=MAC_SECURITY_GROUP_IDS,
            iam_instance_profile_name=EC2_INSTANCE_PROFILE_NAME,
            key_name="awswork",
            user_data_file="./ci/infra/scripts/user_data_macos.txt",
            root_volume_type="gp3",
            root_volume_size=100,
            root_volume_encrypted=True,
            tenancy="host",
            praktika_resource_tag="mac",
            runner_labels=MACOS_AMD_SMALL_RUNNER_LABELS,
            quantity=2,
        ),
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
            # One instance per managed host: 10 hosts minus the one allocated
            # outside praktika. A single instance completes about three fast
            # test runs per hour, so 9 of them absorb the ~18 runs per hour
            # that were queueing up behind 6 instances.
            quantity=9,
        ),
    ]
)
