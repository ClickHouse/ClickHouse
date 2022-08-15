import re
import os
from typing import TextIO, List, Tuple, Optional, Dict

# name, default value, description
Entity = Tuple[str, str, str]

# https://regex101.com/r/R6iogw/12
cmake_option_regex: str = r"^\s*option\s*\(([A-Z_0-9${}]+)\s*(?:\"((?:.|\n)*?)\")?\s*(.*)?\).*$"

ch_master_url: str = "https://github.com/clickhouse/clickhouse/blob/master/"

name_str: str = "<a name=\"{anchor}\"></a>[`{name}`](" + ch_master_url + "{path}#L{line})"
default_anchor_str: str = "[`{name}`](#{anchor})"

comment_var_regex: str = r"\${(.+)}"
comment_var_replace: str = "`\\1`"

table_header: str = """
| Name | Default value | Description | Comment |
|------|---------------|-------------|---------|
"""

# Needed to detect conditional variables (those which are defined twice)
# name -> (path, values)
entities: Dict[str, Tuple[str, str]] = {}


def make_anchor(t: str) -> str:
    return "".join(["-" if i == "_" else i.lower() for i in t if i.isalpha() or i == "_"])

def process_comment(comment: str) -> str:
    return re.sub(comment_var_regex, comment_var_replace, comment, flags=re.MULTILINE)

def build_entity(path: str, entity: Entity, line_comment: Tuple[int, str]) -> None:
    (line, comment) = line_comment
    (name, description, default) = entity

    if name in entities:
        return

    # cannot escape the { in macro option description -> invalid AMP html
    # Skipping "USE_INTERNAL_${LIB_NAME_UC}_LIBRARY"
    if "LIB_NAME_UC" in name:
        return

    if len(default) == 0:
        formatted_default: str = "`OFF`"
    elif default[0] == "$":
        formatted_default: str = "`{}`".format(default[2:-1])
    else:
        formatted_default: str = "`" + default + "`"

    formatted_name: str = name_str.format(
        anchor=make_anchor(name),
        name=name,
        path=path,
        line=line)

    formatted_description: str = "".join(description.split("\n"))

    formatted_comment: str = process_comment(comment)

    formatted_entity: str = "| {} | {} | {} | {} |".format(
        formatted_name, formatted_default, formatted_description, formatted_comment)

    entities[name] = path, formatted_entity

def process_file(root_path: str, file_path: str, file_name: str) -> None:
    with open(os.path.join(file_path, file_name), 'r') as cmake_file:
        contents: str = cmake_file.read()

        def get_line_and_comment(target: str) -> Tuple[int, str]:
            contents_list: List[str] = contents.split("\n")
            comment: str = ""

            for n, line in enumerate(contents_list):
                if 'option' not in line.lower() or target not in line:
                    continue

                for maybe_comment_line in contents_list[n - 1::-1]:
                    if not re.match("\s*#\s*", maybe_comment_line):
                        break

                    comment = re.sub("\s*#\s*", "", maybe_comment_line) + " " + comment

                # line numbering starts with 1
                return n + 1, comment

        matches: Optional[List[Entity]] = re.findall(cmake_option_regex, contents, re.MULTILINE)


        file_rel_path_with_name: str = os.path.join(file_path[len(root_path):], file_name)
        if file_rel_path_with_name.startswith('/'):
            file_rel_path_with_name = file_rel_path_with_name[1:]

        if matches:
            for entity in matches:
                build_entity(file_rel_path_with_name, entity, get_line_and_comment(entity[0]))

def process_folder(root_path: str, name: str) -> None:
    for root, _, files in os.walk(os.path.join(root_path, name)):
        for f in files:
            if f == "CMakeLists.txt" or ".cmake" in f:
                process_file(root_path, root, f)

def generate_cmake_flags_files() -> None:
    root_path: str = os.path.join(os.path.dirname(__file__), '..', '..')

    output_file_name: str = os.path.join(root_path, "docs/en/development/cmake-in-clickhouse.md")
    header_file_name: str = os.path.join(root_path, "docs/_includes/cmake_in_clickhouse_header.md")
    footer_file_name: str = os.path.join(root_path, "docs/_includes/cmake_in_clickhouse_footer.md")

    process_file(root_path, root_path, "CMakeLists.txt")
    process_file(root_path, os.path.join(root_path, "programs"), "CMakeLists.txt")

    process_folder(root_path, "base")
    process_folder(root_path, "cmake")
    process_folder(root_path, "src")

    with open(output_file_name, "w") as f:
        with open(header_file_name, "r") as header:
            f.write(header.read())

        sorted_keys: List[str] = sorted(entities.keys())
        ignored_keys: List[str] = []

        f.write("### ClickHouse modes\n" + table_header)

        for k in sorted_keys:
            if k.startswith("ENABLE_CLICKHOUSE_"):
                f.write(entities[k][1] + "\n")
                ignored_keys.append(k)

        f.write("\n### External libraries\nNote that ClickHouse uses forks of these libraries, see https://github.com/ClickHouse-Extras.\n" +
            table_header)

        for k in sorted_keys:
            if k.startswith("ENABLE_") and ".cmake" in entities[k][0]:
                f.write(entities[k][1] + "\n")
                ignored_keys.append(k)

        f.write("\n\n### External libraries system/bundled mode\n" + table_header)

        for k in sorted_keys:
            if k.startswith("USE_INTERNAL_"):
                f.write(entities[k][1] + "\n")
                ignored_keys.append(k)

        f.write("\n\n### Other flags\n" + table_header)

        for k in sorted(set(sorted_keys).difference(set(ignored_keys))):
            f.write(entities[k][1] + "\n")

        with open(footer_file_name, "r") as footer:
            f.write(footer.read())


if __name__ == '__main__':
    generate_cmake_flags_files()
