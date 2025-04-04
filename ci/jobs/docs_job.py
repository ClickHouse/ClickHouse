from ci.praktika.result import Result
from ci.praktika.utils import Shell

# Get the latest changes from the ClickHouse/clickhouse-docs repo
def fetch_latest_docs_repo_changes():
    git_branch = Shell.check("git branch --show-current", verbose=True, strict=True)

    if  git_branch != "main":
        Shell.run("git branch")
        Shell.run("git checkout main")
    else:
        # Update docs repo
        Shell.run("git pull")

# Install packages and copy the ClickHouse/ClickHouse docs
def run_docs_repo_setup():
    Shell.run("yarn install")
    Shell.run("yarn copy-clickhouse-repo-docs -l docs")

def build_docusaurus():
    res, out, err = Shell.get_res_stdout_stderr(
        "yarn build --locale en"
    )
    if err:
        out += err
    return out

if __name__ == "__main__":

    results = []

    fetch_latest_docs_repo_changes()
    run_docs_repo_setup()

    results.append(
        Result.from_commands_run(
            name="Build Docusaurus", command=build_docusaurus, with_info=True
        )
    )

    Result.create_from(results=results).add_job_summary_to_info(
        with_local_run_command=True
    ).complete_job()
