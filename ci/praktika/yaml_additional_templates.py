class AltinityWorkflowTemplates:
    # Envs not defined in ci/defs/defs.py SECRETS
    # Braces must be escaped
    ADDITIONAL_GLOBAL_ENV = r"""  AWS_DEFAULT_REGION: ${{{{ secrets.AWS_DEFAULT_REGION }}}}
  CHECKS_DATABASE_HOST: ${{{{ secrets.CHECKS_DATABASE_HOST }}}}
  AZURE_STORAGE_KEY: ${{{{ secrets.AZURE_STORAGE_KEY }}}}
  AZURE_ACCOUNT_NAME: ${{{{ secrets.AZURE_ACCOUNT_NAME }}}}
  AZURE_CONTAINER_NAME: ${{{{ secrets.AZURE_CONTAINER_NAME }}}}
  AZURE_STORAGE_ACCOUNT_URL: "https://${{{{ secrets.AZURE_ACCOUNT_NAME }}}}.blob.core.windows.net/"
  ROBOT_TOKEN: ${{{{ secrets.ROBOT_TOKEN }}}}
  GH_TOKEN: ${{{{ github.token }}}}
"""
    # Additional pre steps for all jobs
    JOB_SETUP_STEPS = """
      - name: Setup
        uses: ./.github/actions/runner_setup
"""
    # Additional pre steps for config workflow job
    ADDITIONAL_CI_CONFIG_STEPS = r"""
      - name: Note report location to summary
        env:
          PR_NUMBER: ${{ github.event.pull_request.number || 0 }}
          COMMIT_SHA: ${{ github.event_name == 'pull_request' && github.event.pull_request.head.sha || github.sha }}
        run: |
          REPORT_LINK=https://s3.amazonaws.com/altinity-build-artifacts/$PR_NUMBER/$COMMIT_SHA/ci_run_report.html
          echo "Workflow Run Report: [View Report]($REPORT_LINK)" >> $GITHUB_STEP_SUMMARY
"""
    # Additional jobs
    REGRESSION_HASH = "36ae5e98d627bb18f821d4d72a552e127aeaedc8"
    ADDITIONAL_JOBS = r"""
##########################################################################################
##################################### ALTINITY JOBS ######################################
##########################################################################################
  GrypeScanServer:
    needs: [config_workflow, docker_server_image]
    if: ${{ !failure() && !cancelled() && !contains(fromJson(needs.config_workflow.outputs.data).cache_success_base64, 'RG9ja2VyIHNlcnZlciBpbWFnZQ==') }}
    strategy:
      fail-fast: false
      matrix:
        suffix: ['', '-alpine']
    uses: ./.github/workflows/grype_scan.yml
    secrets: inherit
    with:
      docker_image: altinityinfra/clickhouse-server
      # version: ${{ fromJson(needs.config_workflow.outputs.data).custom_data.version.string }}
      tag-suffix: ${{ matrix.suffix }}
  GrypeScanKeeper:
      needs: [config_workflow, docker_keeper_image]
      if: ${{ !failure() && !cancelled() && !contains(fromJson(needs.config_workflow.outputs.data).cache_success_base64, 'RG9ja2VyIGtlZXBlciBpbWFnZQ==') }}
      uses: ./.github/workflows/grype_scan.yml
      secrets: inherit
      with:
        docker_image: altinityinfra/clickhouse-keeper
        # version: ${{ fromJson(needs.config_workflow.outputs.data).custom_data.version.string }}

  RegressionTestsRelease:
    needs: [config_workflow, build_amd_release]
    if: ${{ !failure() && !cancelled() && !contains(fromJson(needs.config_workflow.outputs.data).cache_success_base64, 'QnVpbGQgKGFtZF9yZWxlYXNlKQ==') && !contains(fromJson(needs.config_workflow.outputs.data).pull_request.body, '[x] <!---ci_exclude_regression-->')}}
    uses: ./.github/workflows/regression.yml
    secrets: inherit
    with:
      runner_type: altinity-on-demand, altinity-regression-tester
      commit: {REGRESSION_HASH}
      arch: release
      build_sha: ${{ github.event_name == 'pull_request' && github.event.pull_request.head.sha || github.sha }}
      timeout_minutes: 300
      workflow_config: ${{ needs.config_workflow.outputs.data }}
  RegressionTestsAarch64:
    needs: [config_workflow, build_arm_release]
    if: ${{ !failure() && !cancelled() && !contains(fromJson(needs.config_workflow.outputs.data).cache_success_base64, 'QnVpbGQgKGFybV9yZWxlYXNlKQ==') && !contains(fromJson(needs.config_workflow.outputs.data).pull_request.body, '[x] <!---ci_exclude_regression-->') && !contains(fromJson(needs.config_workflow.outputs.data).pull_request.body, '[x] <!---ci_exclude_aarch64-->')}}
    uses: ./.github/workflows/regression.yml
    secrets: inherit
    with:
      runner_type: altinity-on-demand, altinity-regression-tester-aarch64
      commit: {REGRESSION_HASH}
      arch: aarch64
      build_sha: ${{ github.event_name == 'pull_request' && github.event.pull_request.head.sha || github.sha }}
      timeout_minutes: 300
      workflow_config: ${{ needs.config_workflow.outputs.data }}

  SignRelease:
    needs: [config_workflow, build_amd_release]
    if: ${{ !failure() && !cancelled() }}
    uses: ./.github/workflows/reusable_sign.yml
    secrets: inherit
    with:
      test_name: Sign release
      runner_type: altinity-style-checker
      data: ${{ needs.config_workflow.outputs.data }}
  SignAarch64:
    needs: [config_workflow, build_arm_release]
    if: ${{ !failure() && !cancelled() }}
    uses: ./.github/workflows/reusable_sign.yml
    secrets: inherit
    with:
      test_name: Sign aarch64
      runner_type: altinity-style-checker-aarch64
      data: ${{ needs.config_workflow.outputs.data }}

  FinishCIReport:
    if: ${{ !cancelled() }}
    needs:
{ALL_JOBS}
      - SignRelease
      - SignAarch64
      - RegressionTestsRelease
      - RegressionTestsAarch64
      - GrypeScanServer
      - GrypeScanKeeper
    runs-on: [self-hosted, altinity-on-demand, altinity-style-checker-aarch64]
    steps:
      - name: Check out repository code
        uses: Altinity/checkout@19599efdf36c4f3f30eb55d5bb388896faea69f6
        with:
          clear-repository: true
      - name: Finalize workflow report
        if: ${{ !cancelled() }}
        uses: ./.github/actions/create_workflow_report
        with:
          workflow_config: ${{ needs.config_workflow.outputs.data }}
          final: true
"""
