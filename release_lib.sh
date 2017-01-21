source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/libs/libcommon/src/get_revision_lib.sh"

function add_daemon_impl {
	local daemon=$1
	local control=$CONTROL
	local dependencies=$2
	local description_short="${daemon%/ daemon}"
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
			add_daemon_impl clickhouse-server-base 'adduser' 'clickhouse-server binary'
			[ -n "$BUILD_PACKAGE_FOR_METRIKA" ] && add_daemon_impl clickhouse-server-metrika "clickhouse-server-base(=1.1.$REVISION)" 'Configuration files specific for Metrika project for clickhouse-server-base package'
			add_daemon_impl clickhouse-server-common "clickhouse-server-base(=1.1.$REVISION)" 'Common configuration files for clickhouse-server-base package'
		;;
		'clickhouse-client' )
			add_daemon_impl clickhouse-client "clickhouse-server-base(=1.1.$REVISION)" "ClickHouse client and additional tools such as clickhouse-local and clickhouse-benchmark."
		;;
		'clickhouse-benchmark' )
			#skip it explicitly
		;;
		'clickhouse-local' )
			#skip it explicitly
		;;
		* )
			add_daemon_impl "${DAEMON_PKG}"
		;;
		esac
	done
}

# Генерируем номер ревизии.
# выставляются переменные окружения REVISION, AUTHOR
function gen_revision_author {
	REVISION=$(get_revision)

	if [[ $STANDALONE != 'yes' ]]
	then
		#needed for libs/libcommon/src/get_revision_lib.sh
		git fetch --tags

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

		auto_message="Auto version update to"
		git_log_grep=`git log --oneline --max-count=1 | grep "$auto_message"`
		if [ "$git_log_grep" == "" ]; then
			git_describe=`git describe`
			sed -i -- "s/VERSION_REVISION .*)/VERSION_REVISION $REVISION)/g" libs/libcommon/cmake/version.cmake
			sed -i -- "s/VERSION_DESCRIBE .*)/VERSION_DESCRIBE $git_describe)/g" libs/libcommon/cmake/version.cmake
			git commit -m "$auto_message [$REVISION]" libs/libcommon/cmake/version.cmake
			# git push
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

# Загрузка в репозитории Метрики
# рабочая директория - где лежит сам скрипт
function upload_debs {
	REVISION="$1"
	DAEMONS="$2"
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
