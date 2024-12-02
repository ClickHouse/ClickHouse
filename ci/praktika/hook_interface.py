from abc import ABC, abstractmethod

from praktika import Workflow


class HookInterface(ABC):
    @abstractmethod
    def pre_run(self, _workflow, _job):
        """
        runs in pre-run step
        :param _workflow:
        :param _job:
        :return:
        """
        pass

    @abstractmethod
    def run(self, _workflow, _job):
        """
        runs in run step
        :param _workflow:
        :param _job:
        :return:
        """
        pass

    @abstractmethod
    def post_run(self, _workflow, _job):
        """
        runs in post-run step
        :param _workflow:
        :param _job:
        :return:
        """
        pass

    @abstractmethod
    def configure(self, _workflow: Workflow.Config):
        """
        runs in initial WorkflowConfig job in run step
        :return:
        """
        pass
