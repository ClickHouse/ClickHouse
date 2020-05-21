#!/bin/bash
set -ex

chown nobody workspace output
chgrp nogroup workspace output
chmod 777 workspace output

cd workspace

# Fetch the repository to find and describe the compared revisions.
rm -rf ch ||:
time git clone --depth 50 --bare https://github.com/ClickHouse/ClickHouse ch
git -C ch fetch origin "$SHA_TO_TEST"

function find_reference_sha
{
    # If not master, try to fetch pull/.../{head,merge}
    if [ "$PR_TO_TEST" != "0" ]
    then
        git -C ch fetch origin "refs/pull/$PR_TO_TEST/*:refs/heads/pr/*"
    fi

    # Go back from the revision to be tested, trying to find the closest published
    # testing release.
    start_ref="$SHA_TO_TEST"~
    # If we are testing a PR, and it merges with master successfully, we are
    # building and testing not the nominal last SHA specified by pull/.../head
    # and SHA_TO_TEST, but a revision that is merged with recent master, given
    # by pull/.../merge ref.
    # Master is the first parent of the pull/.../merge.
    if git -C ch rev-parse pr/merge
    then
        start_ref=pr/merge~
    fi

    while :
    do
        # FIXME the original idea was to compare to a closest testing tag, which
        # is a version that is verified to work correctly. However, we're having
        # some test stability issues now, and the testing release can't roll out
        # for more that a weak already because of that. Temporarily switch to
        # using just closest master, so that we can go on.
        #ref_tag=$(git -C ch describe --match='v*-testing' --abbrev=0 --first-parent "$start_ref")
        ref_tag="$start_ref"

        echo Reference tag is "$ref_tag"
        # We use annotated tags which have their own shas, so we have to further
        # dereference the tag to get the commit it points to, hence the '~0' thing.
        REF_SHA=$(git -C ch rev-parse "$ref_tag~0")

        # FIXME sometimes we have testing tags on commits without published builds --
        # normally these are documentation commits. Loop to skip them.
        if curl --fail --head "https://clickhouse-builds.s3.yandex.net/0/$REF_SHA/performance/performance.tgz"
        then
            break
        fi

        start_ref="$REF_SHA~"
    done

    REF_PR=0
}

# Find reference revision if not specified explicitly
if [ "$REF_SHA" == "" ]; then find_reference_sha; fi
if [ "$REF_SHA" == "" ]; then echo Reference SHA is not specified ; exit 1 ; fi
if [ "$REF_PR" == "" ]; then echo Reference PR is not specified ; exit 1 ; fi

# Show what we're testing
(
    git -C ch log -1 --decorate "$REF_SHA" ||:
) | tee left-commit.txt

(
    git -C ch log -1 --decorate "$SHA_TO_TEST" ||:
    if git -C ch rev-parse pr/merge &> /dev/null
    then
        echo
        echo Real tested commit is:
        git -C ch log -1 --decorate pr/merge
    fi
) | tee right-commit.txt

# Prepare the list of changed tests for use by compare.sh
git -C ch diff --name-only "$SHA_TO_TEST" "$(git -C ch merge-base "$SHA_TO_TEST"~ master)" -- tests/performance | tee changed-tests.txt

# Set python output encoding so that we can print queries with Russian letters.
export PYTHONIOENCODING=utf-8

# By default, use the main comparison script from the tested package, so that we
# can change it in PRs.
script_path="right/scripts"
if [ -v CHPC_LOCAL_SCRIPT ]
then
    script_path=".."
fi

# Even if we have some errors, try our best to save the logs.
set +e

# Use clickhouse-client and clickhouse-local from the right server.
PATH="$(readlink -f right/)":"$PATH"
export PATH

# Start the main comparison script.
{ \
    time ../download.sh "$REF_PR" "$REF_SHA" "$PR_TO_TEST" "$SHA_TO_TEST" && \
    time stage=configure "$script_path"/compare.sh ; \
} 2>&1 | ts "$(printf '%%Y-%%m-%%d %%H:%%M:%%S\t')" | tee compare.log

# Stop the servers to free memory. Normally they are restarted before getting
# the profile info, so they shouldn't use much, but if the comparison script
# fails in the middle, this might not be the case.
for _ in {1..30}
do
    killall clickhouse || break
    sleep 1
done

dmesg -T > dmesg.log

7z a /output/output.7z ./*.{log,tsv,html,txt,rep,svg} {right,left}/{performance,db/preprocessed_configs,scripts} report analyze
cp compare.log /output
