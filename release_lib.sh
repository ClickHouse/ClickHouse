set +e

function get_revision {
    BASEDIR=$(dirname "${BASH_SOURCE[0]}")
    grep "set(VERSION_REVISION" ${BASEDIR}/dbms/cmake/version.cmake | sed 's/^.*VERSION_REVISION \(.*\))$/\1/'
}

function get_author {
    AUTHOR=$(git config --get user.name || echo ${USER})
    echo $AUTHOR
}

# Generate revision number.
# set environment variables REVISION, AUTHOR
function gen_revision_author {
    REVISION=$(get_revision)
    VERSION_PREFIX="${VERSION_PREFIX:-v1.1.}"
    VERSION_POSTFIX="${VERSION_POSTFIX:--testing}"

    if [[ $STANDALONE != 'yes' ]]; then

        git fetch --tags

        succeeded=0
        attempts=0
        max_attempts=1000
        while [ $succeeded -eq 0 ] && [ $attempts -le $max_attempts ]; do
            attempts=$(($attempts + 1))
            REVISION=$(($REVISION + 1))
            git_tag_grep=`git tag | grep "$VERSION_PREFIX$REVISION$VERSION_POSTFIX"`
            if [ "$git_tag_grep" == "" ]; then
                succeeded=1
            fi
        done
        if [ $succeeded -eq 0 ]; then
            echo "Fail to create revision up to $REVISION"
            exit 1
        fi

        auto_message="Auto version update to"
        git_log_grep=`git log --oneline --max-count=1 | grep "$auto_message"`
        if [ "$git_log_grep" == "" ]; then
            tag="$VERSION_PREFIX$REVISION$VERSION_POSTFIX"

            # First tag for correct git describe
            echo -e "\nTrying to create tag: $tag"
            git tag -a "$tag" -m "$tag"

            git_describe=`git describe`
            sed -i -- "s/VERSION_REVISION .*)/VERSION_REVISION $REVISION)/g;s/VERSION_DESCRIBE .*)/VERSION_DESCRIBE $git_describe)/g" dbms/cmake/version.cmake

            gen_changelog "$REVISION" "" "$AUTHOR" ""
            git commit -m "$auto_message [$REVISION]" dbms/cmake/version.cmake debian/changelog
            #git push

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
            REVISION=$(get_revision)
            echo reusing old version $REVISION
        fi

    fi

    AUTHOR=$(git config --get user.name || echo ${USER})
    export REVISION
    export AUTHOR
}

function get_revision_author {
    REVISION=$(get_revision)
    AUTHOR=$(get_author)
    export REVISION
    export AUTHOR
}

# Generate changelog from changelog.in.
# changes
#   programs/CMakeLists.txt
#   dbms/src/CMakeLists.txt
function gen_changelog {
    REVISION="$1"
    CHDATE="$2"
    AUTHOR="$3"
    CHLOG="$4"
    if [ -z "REVISION" ] ; then
        get_revision_author
    fi

    if [ -z "$CHLOG" ] ; then
        CHLOG=debian/changelog
    fi

    if [ -z "$CHDATE" ] ; then
        CHDATE=$(LC_ALL=C date -R | sed -e 's/,/\\,/g') # Replace comma to '\,'
    fi

    sed \
        -e "s/[@]REVISION[@]/$REVISION/g" \
        -e "s/[@]DATE[@]/$CHDATE/g" \
        -e "s/[@]AUTHOR[@]/$AUTHOR/g" \
        -e "s/[@]EMAIL[@]/$(whoami)@yandex-team.ru/g" \
        < $CHLOG.in > $CHLOG
}
