import re
import os.path
from typing import TextIO, List, Tuple, Optional, Dict

Entity = Tuple[str, str, str]

# https://regex101.com/r/R6iogw/11
cmake_option_regex: str = r"^\s*option\s*\(([A-Z_0-9${}]+)\s*(?:\"((?:.|\n)*?)\")?\s*(.*)?\).*$"

output_file_name: str = "cmake_flags_and_output.md"
header_file_name: str = "cmake_files_header.md"
footer_file_name: str = "cmake_files_footer.md"

ch_master_url: str = "https://github.com/clickhouse/clickhouse/blob/master/"

name_str: str = "<a name=\"{anchor}\"></a>[`{name}`](" + ch_master_url + "{path}#L{line})"
default_anchor_str: str = "[`{name}`](#{anchor})"

# Needed to detect conditional variables (those which are defined twice)
entities: Dict[str, str] = {}


def make_anchor(t: str) -> str:
    return "".join(["-" if i == "_" else i.lower() for i in t if i.isalpha() or i == "_"])

def build_entity(path: str, entity: Entity, line_comment: Tuple[int, str], **options) -> None:
    (line, comment) = line_comment
    (_name, _description, default) = entity

    if _name in entities:
        return

    if len(default) == 0:
        default = "`OFF`"
    elif default[0] == "$":
        default = default[2:-1]
        default = default_anchor_str.format(
            name=default,
            anchor=make_anchor(default))
    else:
        default = "`" + default + "`"

    name: str = name_str.format(
        anchor=make_anchor(_name),
        name=_name,
        path=path,
        line=line)

    if options.get("no_desc", False):
        description: str = ""
    else:
        description: str = "".join(_description.split("\n")) + " | "

    entities[name] = "| " + name + " | " + default + " | " + description + comment + " |"

def process_file(input_name: str, **options) -> None:
    with open(input_name, 'r') as cmake_file:
        contents: str = cmake_file.read()

        def get_line_and_comment(target: str) -> Tuple[int, str]:
            contents_list: List[str] = contents.split("\n")
            comment: str = ""

            for n, line in enumerate(contents_list):
                if line.find(target) == -1:
                    continue

                for maybe_comment_line in contents_list[n - 1::-1]:
                    if not re.match("\s*#\s*", maybe_comment_line):
                        break

                    comment = re.sub("\s*#\s*", "", maybe_comment_line) + ". " + comment

                return n, comment

        matches: Optional[List[Entity]] = re.findall(cmake_option_regex, contents, re.MULTILINE)

        if matches:
            for entity in matches:
                build_entity(input_name, entity, get_line_and_comment(entity[0]))

def process_folder(name: str) -> None:
    for root, _, files in os.walk(name):
        for f in files:
            if f == "CMakeLists.txt" or ".cmake" in f:
                process_file(root + "/" + f)

def process() -> None:
    process_file("CMakeLists.txt")
    process_file("programs/CMakeLists.txt", no_desc=True)

    process_folder("base")
    process_folder("cmake")
    process_folder("src")

    with open(output_file_name, "w") as f:
        with open(header_file_name, "r") as header:
            f.write(header.read())

        for k in sorted(entities.keys()):
            f.write(entities[k] + "\n")

        with open(footer_file_name, "r") as footer:
            f.write(footer.read())

process()
