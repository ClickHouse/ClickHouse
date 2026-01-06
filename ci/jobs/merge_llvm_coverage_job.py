import os
from ci.praktika.result import Result
from ci.praktika.utils import Utils

current_directory = Utils.cwd()
temp_dir = f"{current_directory}/ci/tmp/"

if __name__ == "__main__":
    # Pass workspace path to the shell script via environment variable
    os.environ["WORKSPACE_PATH"] = current_directory
    
    result = Result.from_commands_run(
        name="Merge LLVM Coverage",
        command=["bash ci/jobs/merge_llvm_coverage.sh"],
    )
    
    # Compress coverage artifacts
    # Change to temp_dir so archive doesn't include full path
    import subprocess
    subprocess.run(
        f"cd {temp_dir} && tar -czf llvm_coverage_html_report.tar.gz merged.profdata llvm_coverage_html_report",
        shell=True,
        check=True
    )
    
    Result.create_from(
        results=[result],
        files=[f"{temp_dir}/llvm_coverage_html_report.tar.gz"],
        info="LLVM Coverage Merge Job Completed",
    ).complete_job()
