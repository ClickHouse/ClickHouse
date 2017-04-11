function get_revision {
    BASEDIR=$(dirname "${BASH_SOURCE[0]}")
    grep "set(VERSION_REVISION" ${BASEDIR}/dbms/cmake/version.cmake | sed 's/^.*VERSION_REVISION \(.*\))$/\1/'
}

# remove me after fixing all testing-building scripts
function make_control {
    true
}

# Генерируем номер ревизии.
# выставляются переменные окружения REVISION, AUTHOR
function gen_revision_author {
    REVISION=$(get_revision)

    if [[ $STANDALONE != 'yes' ]]
    then
        git fetch --tags

        # Создадим номер ревизии и попытаемся залить на сервер.
        succeeded=0
        attempts=0
        max_attempts=20
        while [ $succeeded -eq 0 ] && [ $attempts -le $max_attempts ]
        do
            REVISION=$(($REVISION + 1))
            attempts=$(($attempts + 1))

            tag="v1.1.$REVISION-testing"

            echo -e "\nTrying to create tag: $tag"
            if git tag -a "$tag" -m "$tag"
            then
                echo -e "\nTrying to push tag to origin: $tag"
                git push origin "$tag"
                if [ $? -ne 0 ]
                then
                    git tag -d "$tag"
                else
                    succeeded=1
                fi
            fi
        done

        if [ $succeeded -eq 0 ]
        then
            echo "Fail to create tag"
            exit 1
        fi

        auto_message="Auto version update to"
        git_log_grep=`git log --oneline --max-count=1 | grep "$auto_message"`
        if [ "$git_log_grep" == "" ]; then
            git_describe=`git describe`
            sed -i -- "s/VERSION_REVISION .*)/VERSION_REVISION $REVISION)/g;s/VERSION_DESCRIBE .*)/VERSION_DESCRIBE $git_describe)/g" dbms/cmake/version.cmake
            git commit -m "$auto_message [$REVISION]" dbms/cmake/version.cmake
            #git push
            tag="v1.1.$REVISION-testing"
            git tag --force -a "$tag" -m "$tag"
            git push --force origin "$tag"
        else
            REVISION=$(get_revision)
            echo reusing old version $REVISION
        fi

    fi

    AUTHOR=$(git config --get user.name)
    export REVISION
    export AUTHOR
}

function get_revision_author {
    REVISION=$(get_revision)
    AUTHOR=$(git config --get user.name)
    export REVISION
    export AUTHOR
}

# Генерируем changelog из changelog.in.
# изменяет
#   programs/CMakeLists.txt
#   dbms/src/CMakeLists.txt
function gen_changelog {
    REVISION="$1"
    CHDATE="$2"
    AUTHOR="$3"
    CHLOG="$4"

    sed \
        -e "s/[@]REVISION[@]/$REVISION/g" \
        -e "s/[@]DATE[@]/$CHDATE/g" \
        -e "s/[@]AUTHOR[@]/$AUTHOR/g" \
        -e "s/[@]EMAIL[@]/$(whoami)@yandex-team.ru/g" \
        < $CHLOG.in > $CHLOG
}

# Загрузка в репозитории Метрики
# рабочая директория - где лежит сам скрипт
function upload_debs {
    REVISION="$1"
    # Определим репозиторий, в который надо загружать пакеты. Он соответствует версии Ubuntu.
    source /etc/lsb-release

    if [ "$DISTRIB_CODENAME" == "precise" ]; then
        REPO="metrika"
    elif [ "$DISTRIB_CODENAME" == "trusty" ]; then
        REPO="metrika-trusty"
    elif [ "$DISTRIB_CODENAME" == "xenial" ]; then
        REPO="metrika-xenial"
    else
        echo -e "\n\e[0;31mUnknown Ubuntu version $DISTRIB_CODENAME \e[0;0m\n"
    fi

    # Загрузка в репозиторий Метрики.

    cd ../
    DUPLOAD_CONF=dupload.conf
    cat src/debian/dupload.conf.in | sed -e "s/[@]AUTHOR[@]/$(whoami)/g" > $DUPLOAD_CONF

    dupload metrika-yandex_1.1."$REVISION"_amd64.changes -t $REPO -c --nomail
}
