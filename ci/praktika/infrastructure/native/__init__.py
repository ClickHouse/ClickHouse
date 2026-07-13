from .cidb_cluster import CIDBCluster
from .configs import lambda_gh_trigger_config, report_page_config
from .github_token_minter import GitHubTokenMinter
from .image_builder import (
    create_awslinux_image_builder_config,
    create_image_test_component,
    create_ubuntu_image_builder_config,
)
from .orchestrator_pool import OrchestratorPool
from .pool_autoscaler import PoolAutoscaler
from .runner_pool import RunnerPool
from .user_data import cidb_user_data


class Components:
    CIDBCluster = CIDBCluster
    GitHubTokenMinter = GitHubTokenMinter
    OrchestratorPool = OrchestratorPool
    PoolAutoscaler = PoolAutoscaler
    RunnerPool = RunnerPool
    report_page_config = report_page_config
    lambda_gh_trigger_config = lambda_gh_trigger_config
    cidb_user_data = staticmethod(cidb_user_data)
    create_awslinux_image_builder_config = staticmethod(
        create_awslinux_image_builder_config
    )
    create_ubuntu_image_builder_config = staticmethod(
        create_ubuntu_image_builder_config
    )
    create_image_test_component = staticmethod(create_image_test_component)
