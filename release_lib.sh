# фильтрует теги, не являющиеся релизными тегами
function tag_filter
{
    grep -E "^v1\.1\.[0-9]{5}-testing$"
}

function add_daemon_impl {
	local daemon=$1
	local control=$CONTROL
	local dependencies=$2
	local description_short="${daemon%-metrika-yandex/ daemon}"
	local description_full=$3

	echo -e "\n\n" >> $control;
	echo "Package: $daemon" >> $control;
	echo "Section: libdevel" >> $control;
	echo "Architecture: any" >> $control;

	echo -n "Depends: \${shlibs:Depends}, \${misc:Depends}" >> $control;
	for dependency in $dependencies
	do
		echo -n ", $dependency" >> $control
	done
	echo >> $control

	echo "Description: $description_short" >> $control;
	echo " $description_full" >> $control;
}

# Создаём файл control из control.in.
# добавляет в файл CONTROL секции для демонов из DAEMONS
function make_control {
	local CONTROL="$1"
	local DAEMONS="$2"
	rm -f $CONTROL
	cp -f $CONTROL.in $CONTROL
	for DAEMON_PKG in $DAEMONS
	do
		case "$DAEMON_PKG" in
		'clickhouse-server' )
			add_daemon_impl clickhouse-server-base '' 'clickhouse-server binary'
			[ -n "$BUILD_PACKAGE_FOR_METRIKA" ] && add_daemon_impl clickhouse-server-metrika "clickhouse-server-base(=1.1.$REVISION)" 'Configuration files specific for Metrika project for clickhouse-server-base package'
			add_daemon_impl clickhouse-server-common "clickhouse-server-base(=1.1.$REVISION)" 'Common configuration files for clickhouse-server-base package'
		;;
		'clickhouse-client' )
			add_daemon_impl clickhouse-client
		;;
		'clickhouse-benchmark' )
			add_daemon_impl clickhouse-benchmark
		;;
		* )
			add_daemon_impl "${DAEMON_PKG}-metrika-yandex"
		;;
		esac
	done
}

# Генерируем номер ревизии.
# выставляются переменные окружения REVISION, AUTHOR
function gen_revision_author {
	# GIT
	git fetch --tags

	REVISION=$(git tag | tag_filter | tail -1 | sed 's/^v1\.1\.\(.*\)-testing$/\1/')

	if [[ $STANDALONE != 'yes' ]]
	then
		# Создадим номер ревизии и попытаемся залить на сервер.
		succeeded=0
		attempts=0
		max_attempts=5
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
	fi

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
	DAEMONS="$5"

	sed \
		-e "s/[@]REVISION[@]/$REVISION/g" \
		-e "s/[@]DATE[@]/$CHDATE/g" \
		-e "s/[@]AUTHOR[@]/$AUTHOR/g" \
		-e "s/[@]EMAIL[@]/$(whoami)@yandex-team.ru/g" \
		< $CHLOG.in > $CHLOG
}

# Загрузка в репозитории Метрики и БК
# рабочая директория - где лежит сам скрипт
function upload_debs {
	REVISION="$1"
	DAEMONS="$2"
	# Определим репозиторий, в который надо загружать пакеты. Он соответствует версии Ubuntu.
	source /etc/lsb-release

	if [ "$DISTRIB_CODENAME" == "precise" ]; then
		REPO="metrika"
		REPO_YABS="bs"
	elif [ "$DISTRIB_CODENAME" == "trusty" ]; then
		REPO="metrika-trusty"
		REPO_YABS="bs-trusty"
	else
		echo -e "\n\e[0;31mUnknown Ubuntu version $DISTRIB_CODENAME \e[0;0m\n"
	fi

	# Загрузка в репозиторий Метрики.

	cd ../
	DUPLOAD_CONF=dupload.conf
	cat src/debian/dupload.conf.in | sed -e "s/[@]AUTHOR[@]/$(whoami)/g" > $DUPLOAD_CONF


	dupload metrika-yandex_1.1."$REVISION"_amd64.changes -t $REPO -c --nomail

	# Загрузка в репозиторий баннерной крутилки (только ClickHouse).
	if [[ -z "$(echo $DAEMONS | tr ' ' '\n' | grep -v clickhouse)" ]];
	then
		echo -e "\n\e[0;32mUploading daemons "$DAEMONS" to Banner System \e[0;0m\n "
		dupload metrika-yandex_1.1."$REVISION"_amd64.changes -t $REPO_YABS -c --nomail
	else
		echo -e "\n\e[0;31mWill not upload daemons to Banner System \e[0;0m\n "
	fi
}
