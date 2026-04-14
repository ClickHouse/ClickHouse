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
"""
    # Additional pre steps for all jobs
    JOB_SETUP_STEPS = """
      - name: Setup
        uses: ./.github/actions/runner_setup
      - name: Docker setup
        uses: ./.github/actions/docker_setup
        with:
          test_name: "{JOB_NAME_GH}"
"""
    # Additional pre steps for config workflow job
    ADDITIONAL_CI_CONFIG_STEPS = r"""
      - name: Note report location to summary
        if: ${{ !failure() && env.AWS_ACCESS_KEY_ID && env.AWS_SECRET_ACCESS_KEY }}
        env:
          PR_NUMBER: ${{ github.event.pull_request.number || 0 }}
          COMMIT_SHA: ${{ github.event_name == 'pull_request' && github.event.pull_request.head.sha || github.sha }}
        run: |
          if [ "$PR_NUMBER" -eq 0 ]; then
            PREFIX="REFs/$GITHUB_REF_NAME/$COMMIT_SHA"
          else
            PREFIX="PRs/$PR_NUMBER/$COMMIT_SHA"
          fi
          REPORT_LINK=https://s3.amazonaws.com/altinity-build-artifacts/$PREFIX/$GITHUB_RUN_ID/ci_run_report.html
          echo "Workflow Run Report: [View Report]($REPORT_LINK)" >> $GITHUB_STEP_SUMMARY
"""
    # Additional jobs
    REGRESSION_HASH = "c7897a6a858a9ef9c7b3c519e7291cfd3c2ec646"
    ALTINITY_JOBS = {
        "GrypeScan": r"""
  GrypeScanServer:
    needs: [config_workflow, docker_server_image]
    if: ${{  !cancelled() && !contains(needs.*.outputs.pipeline_status, 'failure') && !contains(fromJson(needs.config_workflow.outputs.data).workflow_config.cache_success_base64, 'RG9ja2VyIHNlcnZlciBpbWFnZQ==') }}
    strategy:
      fail-fast: false
      matrix:
        suffix: ['', '-alpine']
    uses: ./.github/workflows/grype_scan.yml
    secrets: inherit
    with:
      docker_image: altinityinfra/clickhouse-server
      version: ${{ fromJson(needs.config_workflow.outputs.data).JOB_KV_DATA.version.string }}
      tag-suffix: ${{ matrix.suffix }}
  GrypeScanKeeper:
      needs: [config_workflow, docker_keeper_image]
      if: ${{ !cancelled() && !contains(needs.*.outputs.pipeline_status, 'failure') && !contains(fromJson(needs.config_workflow.outputs.data).workflow_config.cache_success_base64, 'RG9ja2VyIGtlZXBlciBpbWFnZQ==') }}
      uses: ./.github/workflows/grype_scan.yml
      secrets: inherit
      with:
        docker_image: altinityinfra/clickhouse-keeper
        version: ${{ fromJson(needs.config_workflow.outputs.data).JOB_KV_DATA.version.string }}
""",
        "Regression": r"""
  RegressionTestsRelease:
    needs: [config_workflow, build_amd_binary, stateless_tests_amd_debug_parallel]
    if: ${{  !cancelled() && !contains(needs.*.outputs.pipeline_status, 'failure') && !contains(fromJson(needs.config_workflow.outputs.data).JOB_KV_DATA.ci_exclude_tags, 'regression')}}
    uses: ./.github/workflows/regression.yml
    secrets: inherit
    with:
      runner_type: altinity-regression-tester
      commit: {REGRESSION_HASH}
      arch: release
      build_sha: ${{ github.event_name == 'pull_request' && github.event.pull_request.head.sha || github.sha }}
      timeout_minutes: 210
      workflow_config: ${{ needs.config_workflow.outputs.data }}
  RegressionTestsAarch64:
    needs: [config_workflow, build_arm_binary, stateless_tests_arm_binary_parallel]
    if: ${{  !cancelled() && !contains(needs.*.outputs.pipeline_status, 'failure') && !contains(fromJson(needs.config_workflow.outputs.data).JOB_KV_DATA.ci_exclude_tags, 'regression') && !contains(fromJson(needs.config_workflow.outputs.data).workflow_config.custom_data.ci_exclude_tags, 'aarch64')}}
    uses: ./.github/workflows/regression.yml
    secrets: inherit
    with:
      runner_type: altinity-regression-tester-aarch64
      commit: {REGRESSION_HASH}
      arch: aarch64
      build_sha: ${{ github.event_name == 'pull_request' && github.event.pull_request.head.sha || github.sha }}
      timeout_minutes: 210
      workflow_config: ${{ needs.config_workflow.outputs.data }}
""",
        "SignRelease": r"""
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
""",
        "CIReport": r"""
  FinishCIReport:
    if: ${{ !cancelled() }}
    needs:
{ALL_JOBS}
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
          workflow_config: ${{ toJson(needs) }}
          final: true
""",
        "SourceUpload": r"""
  SourceUpload:
    needs: [config_workflow, build_amd_release]
    if: ${{ !failure() && !cancelled() }}
    runs-on: [self-hosted, altinity-on-demand, altinity-style-checker-aarch64]
    env:
        COMMIT_SHA: ${{ github.event_name == 'pull_request' && github.event.pull_request.head.sha || github.sha }}
        PR_NUMBER: ${{ github.event.pull_request.number || 0 }}
        VERSION: ${{ fromJson(needs.config_workflow.outputs.data).JOB_KV_DATA.version.string }}
    steps:
      - name: Check out repository code
        uses: Altinity/checkout@19599efdf36c4f3f30eb55d5bb388896faea69f6
        with:
          clear-repository: true
          ref: ${{ fromJson(needs.config_workflow.outputs.data).git_ref }}
          submodules: true
          fetch-depth: 0
          filter: tree:0
      - name: Install aws cli
        uses: unfor19/install-aws-cli-action@v1
        with:
          version: 2
          arch: arm64
      - name: Create source tar
        run: |
          cd .. && tar czf $RUNNER_TEMP/build_source.src.tar.gz ClickHouse/
      - name: Upload source tar
        run: |
          if [ "$PR_NUMBER" -eq 0 ]; then
              S3_PATH="REFs/$GITHUB_REF_NAME/$COMMIT_SHA/build_amd_release"
          else
              S3_PATH="PRs/$PR_NUMBER/$COMMIT_SHA/build_amd_release"
          fi

          aws s3 cp $RUNNER_TEMP/build_source.src.tar.gz s3://altinity-build-artifacts/$S3_PATH/clickhouse-$VERSION.src.tar.gz
""",
    }
    ADDITIONAL_JOBS_BANNER = r"""
##########################################################################################
##################################### ALTINITY JOBS ######################################
##########################################################################################
"""
