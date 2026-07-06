from praktika import Artifact, Job

from ci.defs.defs import TEMP_DIR, ArtifactNames, RunnerLabels


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
