from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError


class KazooClientWithImplicitRetries(KazooClient):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def exists(self, *args, **kwargs):
        return self.retry(super().exists, *args, **kwargs)

    def create(self, *args, **kwargs):
        # `create` is not idempotent: a previous attempt may have committed on
        # the server while the response was lost (e.g. `ConnectionLoss` during
        # cluster reconfig). The retry then sees `NodeExistsError`. Treat that
        # case as success only when we have retried at least once and the
        # existing value matches what we tried to write.
        if args:
            path = args[0]
            value = args[1] if len(args) >= 2 else kwargs.get("value", b"")
        else:
            path = kwargs.get("path")
            value = kwargs.get("value", b"")
        include_data = kwargs.get("include_data", False)
        attempt_count = [0]

        def do_create():
            attempt_count[0] += 1
            try:
                return KazooClient.create(self, *args, **kwargs)
            except NodeExistsError:
                # First-attempt `NodeExistsError` must remain a hard error so
                # tests that intentionally probe "already exists" still see it.
                if attempt_count[0] > 1 and path is not None:
                    try:
                        existing_value, existing_stat = KazooClient.get(self, path)
                        if existing_value == value:
                            return (
                                (path, existing_stat) if include_data else path
                            )
                    except Exception:
                        pass
                raise

        return self.retry(do_create)

    def delete(self, *args, **kwargs):
        return self.retry(super().delete, *args, **kwargs)

    def get(self, *args, **kwargs):
        return self.retry(super().get, *args, **kwargs)

    def get_children(self, *args, **kwargs):
        return self.retry(super().get_children, *args, **kwargs)

    def set(self, *args, **kwargs):
        return self.retry(super().set, *args, **kwargs)
