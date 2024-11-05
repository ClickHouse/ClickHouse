from dataclasses import dataclass


class Artifact:
    class Type:
        GH = "github"
        S3 = "s3"
        PHONY = "phony"

    @dataclass
    class Config:
        """
        name - artifact name
        type - artifact type, see Artifact.Type
        path - file path or glob, e.g. "path/**/[abc]rtifac?/*"
        """

        name: str
        type: str
        path: str
        _provided_by: str = ""
        _s3_path: str = ""

        def is_s3_artifact(self):
            return self.type == Artifact.Type.S3

    @classmethod
    def define_artifact(cls, name, type, path):
        return cls.Config(name=name, type=type, path=path)

    @classmethod
    def define_gh_artifact(cls, name, path):
        return cls.define_artifact(name=name, type=cls.Type.GH, path=path)
