from praktika import Artifact, Job

from ci.defs.defs import TEMP_DIR, ArtifactNames, RunnerLabels
from ci.defs.job_configs import common_ft_job_config


class AltinityArtifactNames:
    SIGNED_AMD_RELEASE = "SIGNED_AMD_RELEASE"
    SIGNED_ARM_RELEASE = "SIGNED_ARM_RELEASE"


class AltinityArtifactConfigs:
    signed_hashes = Artifact.Config(
        name="*",
        type=Artifact.Type.S3,
        path=f"{TEMP_DIR}/*.gpg",
    ).parametrize(
        names=[
            AltinityArtifactNames.SIGNED_AMD_RELEASE,
            AltinityArtifactNames.SIGNED_ARM_RELEASE,
        ]
    )


class AltinityJobNames:
    SIGN_RELEASE = "Sign release"
    SOURCE_UPLOAD = "Source upload"


class AltinityJobConfigs:
    sign_release_jobs = Job.Config(
        name=AltinityJobNames.SIGN_RELEASE,
        runs_on=[],  # from parametrize()
        command="python3 ./ci/jobs/sign_release.py",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/jobs/sign_release.py",
            ],
        ),
        timeout=900,
    ).parametrize(
        Job.ParamSet(
            parameter="amd_release",
            runs_on=RunnerLabels.STYLE_CHECK_AMD,
            requires=[
                ArtifactNames.DEB_AMD_RELEASE,
                ArtifactNames.RPM_AMD_RELEASE,
                ArtifactNames.TGZ_AMD_RELEASE,
                ArtifactNames.CH_AMD_RELEASE,
            ],
            provides=[AltinityArtifactNames.SIGNED_AMD_RELEASE],
        ),
        Job.ParamSet(
            parameter="arm_release",
            runs_on=RunnerLabels.STYLE_CHECK_AMD,
            requires=[
                ArtifactNames.DEB_ARM_RELEASE,
                ArtifactNames.RPM_ARM_RELEASE,
                ArtifactNames.TGZ_ARM_RELEASE,
                ArtifactNames.CH_ARM_RELEASE,
            ],
            provides=[AltinityArtifactNames.SIGNED_ARM_RELEASE],
        ),
    )
    # No digest_config: the job tars the whole source tree, which changes every
    # commit, so it must never be skipped by the content-addressed CI cache.
    source_upload_job = Job.Config(
        name=AltinityJobNames.SOURCE_UPLOAD,
        runs_on=RunnerLabels.STYLE_CHECK_AMD,
        command="python3 ./ci/jobs/source_upload.py",
        timeout=3600,
    )
    # Stateless tests with a content-addressed disk as the default MergeTree storage.
    cas_functional_tests_jobs = common_ft_job_config.parametrize(
        # CAS over S3: RustFS, not MinIO OSS, because the incarnation pool needs
        # enforced conditional deletes.
        Job.ParamSet(
            parameter="amd_binary, cas s3 storage, parallel",
            runs_on=RunnerLabels.AMD_MEDIUM_CPU,
            requires=[ArtifactNames.CH_AMD_BINARY_GH],
        ),
        # The sanitizer lanes are sharded because an unsharded one exceeds the 6h
        # GitHub job timeout and is killed before it uploads any results.
        *[
            Job.ParamSet(
                parameter=f"amd_asan_ubsan, cas s3 storage, parallel, {batch}/{total_batches}",
                runs_on=RunnerLabels.AMD_MEDIUM_CPU,
                requires=[ArtifactNames.CH_AMD_ASAN_UBSAN_GH],
            )
            for total_batches in (2,)
            for batch in range(1, total_batches + 1)
        ],
        *[
            Job.ParamSet(
                parameter=f"amd_tsan, cas s3 storage, parallel, {batch}/{total_batches}",
                runs_on=RunnerLabels.AMD_MEDIUM,
                requires=[ArtifactNames.CH_AMD_TSAN_GH],
            )
            for total_batches in (2,)
            for batch in range(1, total_batches + 1)
        ],
        *[
            Job.ParamSet(
                parameter=f"amd_msan, cas s3 storage, parallel, {batch}/{total_batches}",
                runs_on=RunnerLabels.FUNC_TESTER_AMD,
                requires=[ArtifactNames.CH_AMD_MSAN_GH],
            )
            for total_batches in (3,)
            for batch in range(1, total_batches + 1)
        ],
        Job.ParamSet(
            parameter="arm_binary, cas s3 storage, parallel",
            runs_on=RunnerLabels.ARM_MEDIUM_CPU,
            requires=[ArtifactNames.CH_ARM_BINARY_GH],
        ),
        # CAS over local object storage.
        Job.ParamSet(
            parameter="amd_binary, cas storage, parallel",
            runs_on=RunnerLabels.AMD_MEDIUM_CPU,
            requires=[ArtifactNames.CH_AMD_BINARY_GH],
        ),
    )
