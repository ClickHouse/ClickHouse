from jinja2 import Environment, FileSystemLoader
from defs import *

LineWithHit = tuple[Line, bool]
FuncWithHit = tuple[FuncName, Line, bool]

class TestFile:
    def __init__(self, name, lines, functions):
        self.name: str = name
        self.lines: list[LineWithHit] = lines
        self.functions: list[FuncWithHit] = functions

def _prepare_data(files: list[SourceFile], tests: list[Test], tests_names: list[str]) -> list[TestFile]:
    pass

def generate_html(out_dir: str, files, tests, tests_names):
    file_loader = FileSystemLoader('jinja')

    env = Environment(loader=file_loader)
    env.trim_blocks = True
    env.lstrip_blocks = True
    env.rstrip_blocks = True

    template = env.get_template('index.html')

    data = _prepare_data(files, tests, tests_names)

    output = template.render(data=data)

    with open(out_dir + '/index.html', "w") as f:
        f.write(output)
