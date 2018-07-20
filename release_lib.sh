set +e

function gen_version_string {
    if [ -n "$TEST" ]; then
        VERSION_STRING="$VERSION_MAJOR.$VERSION_MINOR.$VERSION_PATCH.$VERSION_REVISION"
    else
        VERSION_STRING="$VERSION_MAJOR.$VERSION_MINOR.$VERSION_PATCH"
    fi
}

function get_version {
    BASEDIR=$(dirname "${BASH_SOURCE[0]}")
    VERSION_REVISION=`grep "set(VERSION_REVISION" ${BASEDIR}/dbms/cmake/version.cmake | sed 's/^.*VERSION_REVISION \(.*\)$/\1/' | sed 's/[) ].*//'`
    VERSION_MAJOR=`grep "set(VERSION_MAJOR" ${BASEDIR}/dbms/cmake/version.cmake | sed 's/^.*VERSION_MAJOR \(.*\)/\1/' | sed 's/[) ].*//'`
    VERSION_MINOR=`grep "set(VERSION_MINOR" ${BASEDIR}/dbms/cmake/version.cmake | sed 's/^.*VERSION_MINOR \(.*\)/\1/' | sed 's/[) ].*//'`
    VERSION_PATCH=`grep "set(VERSION_PATCH" ${BASEDIR}/dbms/cmake/version.cmake | sed 's/^.*VERSION_PATCH \(.*\)/\1/' | sed 's/[) ].*//'`
    VERSION_PREFIX="${VERSION_PREFIX:-v}"
    VERSION_POSTFIX="${VERSION_POSTFIX:--testing}"

    gen_version_string
}

function get_author {
    AUTHOR=$(git config --get user.name || echo ${USER})
    echo $AUTHOR
}

# Generate revision number.
# set environment variables REVISION, AUTHOR
function gen_revision_author {
    TYPE=$1
    get_version

    if [[ $STANDALONE != 'yes' ]]; then

        git fetch --tags

        succeeded=0
        attempts=0
        max_attempts=1000
        while [ $succeeded -eq 0 ] && [ $attempts -le $max_attempts ]; do
            attempts=$(($attempts + 1))

            if [ "$TYPE" == "major" ]; then
                VERSION_REVISION=$(($VERSION_REVISION + 1))
                VERSION_MAJOR=$(($VERSION_MAJOR + 1))
                VERSION_MINOR=1
                VERSION_PATCH=0
            elif [ "$TYPE" == "minor" ] || [ "$TYPE" == "" ]; then
                VERSION_REVISION=$(($VERSION_REVISION + 1))
                VERSION_MINOR=$(($VERSION_MINOR + 1))
                VERSION_PATCH=0
            elif [ "$TYPE" == "patch" ] || [ "$TYPE" == "bugfix" ]; then
                # VERSION_REVISION not incremented.
                VERSION_PATCH=$(($VERSION_PATCH + 1))
            else
                echo "Unknown version type $TYPE"
                exit 1
            fi

            gen_version_string

            git_tag_grep=`git tag | grep "$VERSION_PREFIX$VERSION_STRING$VERSION_POSTFIX"`
            if [ "$git_tag_grep" == "" ]; then
                succeeded=1
            fi
        done
        if [ $succeeded -eq 0 ]; then
            echo "Fail to create revision up to $VERSION_REVISION"
            exit 1
        fi

        auto_message="Auto version update to"
        git_log_grep=`git log --oneline --max-count=1 | grep "$auto_message"`
        if [ "$git_log_grep" == "" ]; then
            tag="$VERSION_PREFIX$VERSION_STRING$VERSION_POSTFIX"

            # First tag for correct git describe
            echo -e "\nTrying to create tag: $tag"
            git tag -a "$tag" -m "$tag"

            git_describe=`git describe`
            git_hash=`git rev-parse HEAD`
            sed -i -e "s/set(VERSION_REVISION [^) ]*/set(VERSION_REVISION $VERSION_REVISION/g;" \
                -e "s/set(VERSION_DESCRIBE [^) ]*/set(VERSION_DESCRIBE $git_describe/g;" \
                -e "s/set(VERSION_GITHASH [^) ]*/set(VERSION_GITHASH $git_hash/g;" \
                -e "s/set(VERSION_MAJOR [^) ]*/set(VERSION_MAJOR $VERSION_MAJOR/g;" \
                -e "s/set(VERSION_MINOR [^) ]*/set(VERSION_MINOR $VERSION_MINOR/g;" \
                -e "s/set(VERSION_PATCH [^) ]*/set(VERSION_PATCH $VERSION_PATCH/g;" \
                -e "s/set(VERSION_STRING [^) ]*/set(VERSION_STRING $VERSION_STRING/g;" \
                dbms/cmake/version.cmake

            gen_changelog "$VERSION_STRING" "" "$AUTHOR" ""
            git commit -m "$auto_message [$VERSION_STRING] [$VERSION_REVISION]" dbms/cmake/version.cmake debian/changelog
            git push

            # Second tag for correct version information in version.cmake inside tag
            if git tag --force -a "$tag" -m "$tag"
            then
                echo -e "\nTrying to push tag to origin: $tag"
                git push origin "$tag"
                if [ $? -ne 0 ]
                then
                    git tag -d "$tag"
                    echo "Fail to create tag"
                    exit 1
                fi
            fi

        else
            get_version
            echo reusing old version $VERSION_STRING
        fi

    fi

    AUTHOR=$(git config --get user.name || echo ${USER})
    export AUTHOR
}

function get_revision_author {
    get_version
    AUTHOR=$(get_author)
    export AUTHOR
}

# Generate changelog from changelog.in.
function gen_changelog {
    VERSION_STRING="$1"
    CHDATE="$2"
    AUTHOR="$3"
    CHLOG="$4"
    if [ -z "$VERSION_STRING" ] ; then
        get_revision_author
    fi

    if [ -z "$CHLOG" ] ; then
        CHLOG=debian/changelog
    fi

    if [ -z "$CHDATE" ] ; then
        CHDATE=$(LC_ALL=C date -R | sed -e 's/,/\\,/g') # Replace comma to '\,'
    fi

    sed \
        -e "s/[@]VERSION_STRING[@]/$VERSION_STRING/g" \
        -e "s/[@]DATE[@]/$CHDATE/g" \
        -e "s/[@]AUTHOR[@]/$AUTHOR/g" \
        -e "s/[@]EMAIL[@]/$(whoami)@yandex-team.ru/g" \
        < $CHLOG.in > $CHLOG
}
