import sys

from praktika.info import Info

from ci.jobs.scripts.workflow_hooks.pr_labels_and_category import Labels

# Files that contain embedded documentation as part of their definitions
embedded_doc_files = [
    "src/Common/ProfileEvents.cpp",
    "src/Common/CurrentMetrics.cpp",
]

# Directories where source files may contain inline documentation
# (e.g. FunctionDocumentation structs registered with function factories)
inline_doc_paths = [
    "src/Functions/",
    "src/AggregateFunctions/",
    "src/TableFunctions/",
]

# File name suffixes that indicate embedded documentation
# (e.g. settings files with DECLARE macros containing description strings)
embedded_doc_suffixes = [
    "Settings.cpp",
]


def file_contains_marker(file_path, marker):
    """Check if a file contains the given marker string."""
    try:
        with open(file_path) as f:
            content = f.read()
        return marker in content
    except OSError:
        return False


def check_docs():
    info = Info()
    if Labels.PR_FEATURE in info.pr_labels:
        changed_files = info.get_kv_data("changed_files")
        has_doc_changes = any(
            file.startswith("docs/")
            or file in embedded_doc_files
            or (
                any(file.startswith(path) for path in inline_doc_paths)
                and file_contains_marker(file, "FunctionDocumentation")
            )
            or (
                any(file.endswith(suffix) for suffix in embedded_doc_suffixes)
                and file_contains_marker(file, "DECLARE_SETTING")
            )
            for file in changed_files
        )
        if not has_doc_changes:
            print(
                "No documentation changes found, please update the documentation"
            )
            return False
    return True


if __name__ == "__main__":
    if not check_docs():
        sys.exit(1)
