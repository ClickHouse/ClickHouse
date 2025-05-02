from abc import abstractmethod
import json
import pathlib
import random
import sys
import tempfile

from integration.helpers.client import CommandRequest
from integration.helpers.cluster import ClickHouseInstance

class Generator():
    def __init__(self, binary : pathlib.Path, config : pathlib.Path):
        self.binary : pathlib.Path = binary
        self.config : pathlib.Path = config

    @abstractmethod
    def run_generator(self) -> CommandRequest:
        pass

class BuzzHouseGenerator(Generator):
    def __init__(self, binary : pathlib.Path, config : pathlib.Path):
        super().__init__(binary, config)

        # Load configuration
        buzz_config = {}
        self.temp = tempfile.NamedTemporaryFile()
        if config is not None:
            with open(config, 'r') as file1:
                buzz_config  = json.load(file1)

        buzz_config['seed'] = random.randint(1, 18446744073709551615)
        with open(self.temp.name, "w") as file2:
            file2.write(json.dumps(buzz_config))

    def run_generator(self, server : ClickHouseInstance) -> CommandRequest:
        return CommandRequest([self.binary, "--client", "--host", f"{server.ip_address}", "--port", "9000", f"--buzz-house-config={self.temp.name}"],
                              stdin = "",
                              timeout = None,
                              ignore_error = True,
                              parse = False,
                              stdout = sys.stdout,
                              stderr = sys.stderr)
