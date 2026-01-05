import os
from ci.praktika.result import Result
from ci.praktika.utils import Utils

current_directory = Utils.cwd()
temp_dir = f"{current_directory}/ci/tmp/"

if __name__ == "__main__":
    # Pass workspace path to the shell script
    env = os.environ.copy()
    env["WORKSPACE_PATH"] = current_directory
    
    result = Result.from_commands_run(
        name="Merge LLVM Coverage",
        command=["bash ci/jobs/merge_llvm_coverage.sh"],
        env=env,
    )
    
    attached_files = []
    
    # Compress coverage artifacts
    coverage_files = [
        f"{temp_dir}/merged.profdata",
        f"{temp_dir}/llvm_coverage_html_report",
    ]
    attached_files.append(
        Utils.compress_files_gz(coverage_files, f"{temp_dir}/llvm_coverage_html_report.tar.gz")
    )
    
    Result.create_from(
        results=[result],
        files=attached_files,
        info="LLVM Coverage Merge Job Completed",
    ).complete_job()
