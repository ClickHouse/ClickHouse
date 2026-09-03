from kazoo.client import KazooClient


class KazooClientWithImplicitRetries(KazooClient):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def exists(self, *args, **kwargs):
        return self.retry(super().exists, *args, **kwargs)

    def create(self, *args, **kwargs):
        return self.retry(super().create, *args, **kwargs)

    def delete(self, *args, **kwargs):
        return self.retry(super().delete, *args, **kwargs)

    def get(self, *args, **kwargs):
        return self.retry(super().get, *args, **kwargs)

    def get_children(self, *args, **kwargs):
        return self.retry(super().get_children, *args, **kwargs)

    def set(self, *args, **kwargs):
        return self.retry(super().set, *args, **kwargs)
