import copy
from dataclasses import dataclass, field
from typing import Any, Dict, List, Union


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
        path: Union[str, List[str]]
        compress_zst: bool = False
        _provided_by: str = ""
        ext: Dict[str, Any] = field(default_factory=dict)

        def is_s3_artifact(self):
            return self.type == Artifact.Type.S3

        def is_phony(self):
            return self.type == Artifact.Type.PHONY

        def parametrize(self, names):
            res = []
            for name in names:
                obj = copy.deepcopy(self)
                obj.name = name
                res.append(obj)
            return res

        def add_tags(self, tags: Dict[str, str]):
            """
            Add tags to the artifact. Only S3 artifacts support tags.
            Returns a copy of the artifact with the tags added.
            :param tags: Dictionary of tag key-value pairs
            :return: A new Config object with the tags added
            :raises ValueError: If artifact type is not S3
            """
            if not self.is_s3_artifact():
                raise ValueError(
                    f"Tags can only be added to S3 artifacts, but artifact type is [{self.type}]"
                )
            obj = copy.deepcopy(self)
            if "tags" not in obj.ext:
                obj.ext["tags"] = {}
            obj.ext["tags"].update(tags)
            return obj
