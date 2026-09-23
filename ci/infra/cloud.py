from praktika import CloudInfrastructure
from praktika.infrastructure.dedicated_host import DedicatedHost
from praktika.infrastructure.ec2_instance import EC2Instance
from praktika.infrastructure.iam_instance_profile import IAMInstanceProfile

from ci.defs.defs import RunnerLabels

MAC_OS_TAHOE_IMAGE_AMI = "ami-0f8ce53a93ab42329"
MAC_OS_SEQUOIA_AMD_IMAGE_AMI = "ami-0d66088cfc49c54b0"
MAC_VPC_NAME = "ci-cd"
MAC_SECURITY_GROUP_IDS = ["sg-061fd9184274476c0"]

PRAKTIKA_EC2_ROLE_NAME = "praktika-ec2-role"
PRAKTIKA_EC2_INSTANCE_PROFILE_NAME = "praktika-ec2-instance-profile"

CLOUD = CloudInfrastructure.Config(
    name="cloud_ci_infra",
    lambda_functions=[*CloudInfrastructure.SLACK_APP_LAMBDAS],
    iam_instance_profiles=[
        IAMInstanceProfile.Config(
            name=PRAKTIKA_EC2_INSTANCE_PROFILE_NAME,
            role_name=PRAKTIKA_EC2_ROLE_NAME,
            policy_arns=[
                "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore",
                "arn:aws:iam::aws:policy/AmazonSSMReadOnlyAccess",
                "arn:aws:iam::aws:policy/CloudWatchAgentServerPolicy",
            ],
            inline_policies={
                "PraktikaRunnerRestrictedAccess": {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Sid": "S3ReadWrite",
                            "Effect": "Allow",
                            "Action": [
                                "s3:GetObject",
                                "s3:GetObjectTagging",
                                "s3:HeadObject",
                                "s3:ListBucket",
                                "s3:GetBucketLocation",
                                "s3:PutObject",
                                "s3:PutObjectTagging",
                                "s3:AbortMultipartUpload",
                                "s3:ListBucketMultipartUploads",
                                "s3:ListMultipartUploadParts",
                            ],
                            "Resource": [
                                "arn:aws:s3:::*",
                                "arn:aws:s3:::*/*",
                            ],
                        },
                        {
                            "Sid": "SecretsManagerRead",
                            "Effect": "Allow",
                            "Action": [
                                "secretsmanager:DescribeSecret",
                                "secretsmanager:GetSecretValue",
                            ],
                            "Resource": "arn:aws:secretsmanager:*:*:secret:woolenwolf_gh_app*",
                        },
                        {
                            "Sid": "AutoScalingReadAndScaleIn",
                            "Effect": "Allow",
                            "Action": [
                                "autoscaling:Describe*",
                                "autoscaling:TerminateInstanceInAutoScalingGroup",
                            ],
                            "Resource": "*",
                        },
                        {
                            "Sid": "EC2TerminateOnly",
                            "Effect": "Allow",
                            "Action": [
                                "ec2:Describe*",
                                "ec2:TerminateInstances",
                            ],
                            "Resource": "*",
                        },
                    ],
                }
            },
        )
    ],
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
    ],
    ec2_instances=[
        EC2Instance.Config(
            name=RunnerLabels.MACOS_AMD_SMALL[1],
            image_id=MAC_OS_SEQUOIA_AMD_IMAGE_AMI,
            instance_type="mac1.metal",
            subnet_id="subnet-0a3886c4db842da5b",
            security_group_ids=MAC_SECURITY_GROUP_IDS,
            iam_instance_profile_name=PRAKTIKA_EC2_INSTANCE_PROFILE_NAME,
            key_name="awswork",
            user_data_file="./ci/infra/scripts/user_data_macos.txt",
            root_volume_type="gp3",
            root_volume_size=100,
            root_volume_encrypted=True,
            tenancy="host",
            praktika_resource_tag="mac",
            runner_type=RunnerLabels.MACOS_AMD_SMALL[1],
            quantity=2,
        ),
    ],
    # TODO: add autoscaling and launch templates
    # from praktika.infrastructure.autoscaling_group import AutoScalingGroup
    # from praktika.infrastructure.launch_template import LaunchTemplate
    # launch_templates=[
    #     LaunchTemplate.Config(
    #         name="praktika-hello-world-lt",
    #         image_id=MAC_OS_TAHOE_IMAGE_AMI,
    #         instance_type="mac2-m2.metal",
    #         security_group_ids=MAC_SECURITY_GROUP_IDS,
    #         set_default_version_to_latest=True,
    #         tenancy="host",
    #         user_data="#!/bin/bash\necho hello-world\n",
    #         praktika_resource_tag="mac",
    #         runner_type=RunnerLabels.MACOS_ARM_SMALL[1],
    #     )
    # ],
    # autoscaling_groups=[
    #     AutoScalingGroup.Config(
    #         name="praktika-mac-asg",
    #         vpc_name=MAC_VPC_NAME,
    #         availability_zones=["us-east-1a"],
    #         min_size=1,
    #         max_size=1,
    #         desired_capacity=1,
    #         launch_template_name="praktika-hello-world-lt",
    #         launch_template_version="$Latest",
    #         praktika_resource_tag="mac",
    #         runner_type=RunnerLabels.MACOS_ARM_SMALL[1],
    #     )
    # ],
)
