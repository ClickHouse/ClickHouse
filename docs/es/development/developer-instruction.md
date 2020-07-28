---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 61
toc_title: "La instrucci\xF3n para desarrolladores de ClickHouse para principiantes"
---

La construcción de ClickHouse es compatible con Linux, FreeBSD y Mac OS X.

# Si utiliza Windows {#if-you-use-windows}

Si usa Windows, necesita crear una máquina virtual con Ubuntu. Para comenzar a trabajar con una máquina virtual, instale VirtualBox. Puede descargar Ubuntu desde el sitio web: https://www.ubuntu.com/\#download. Por favor, cree una máquina virtual a partir de la imagen descargada (debe reservar al menos 4 GB de RAM para ello). Para ejecutar un terminal de línea de comandos en Ubuntu, busque un programa que contenga la palabra “terminal” en su nombre (gnome-terminal, konsole etc.) o simplemente presione Ctrl + Alt + T.

# Si utiliza un sistema de 32 bits {#if-you-use-a-32-bit-system}

ClickHouse no puede funcionar ni construir en un sistema de 32 bits. Debe adquirir acceso a un sistema de 64 bits y puede continuar leyendo.

# Creación de un repositorio en GitHub {#creating-a-repository-on-github}

Para comenzar a trabajar con el repositorio de ClickHouse, necesitará una cuenta de GitHub.

Probablemente ya tenga uno, pero si no lo hace, regístrese en https://github.com . En caso de que no tenga claves SSH, debe generarlas y luego cargarlas en GitHub. Es necesario para enviar a través de sus parches. También es posible usar las mismas claves SSH que usa con cualquier otro servidor SSH, probablemente ya las tenga.

Cree una bifurcación del repositorio ClickHouse. Para hacerlo por favor haga clic en el “fork” botón en la esquina superior derecha en https://github.com/ClickHouse/ClickHouse . Se bifurcará su propia copia de ClickHouse/ClickHouse a su cuenta.

El proceso de desarrollo consiste en comprometer primero los cambios previstos en su bifurcación de ClickHouse y luego crear un “pull request” para que estos cambios sean aceptados en el repositorio principal (ClickHouse / ClickHouse).

Para trabajar con repositorios git, instale `git`.

Para hacer eso en Ubuntu, ejecutaría en la terminal de línea de comandos:

    sudo apt update
    sudo apt install git

Puede encontrar un breve manual sobre el uso de Git aquí: https://services.github.com/on-demand/downloads/github-git-cheat-sheet.pdf .
Para obtener un manual detallado sobre Git, consulte https://git-scm.com/book/en/v2 .

# Clonación de un repositorio en su máquina de desarrollo {#cloning-a-repository-to-your-development-machine}

A continuación, debe descargar los archivos fuente en su máquina de trabajo. Esto se llama “to clone a repository” porque crea una copia local del repositorio en su máquina de trabajo.

En el terminal de línea de comandos, ejecute:

    git clone --recursive git@github.com:your_github_username/ClickHouse.git
    cd ClickHouse

Nota: por favor, sustituye *your\_github\_username* con lo que es apropiado!

Este comando creará un directorio `ClickHouse` que contiene la copia de trabajo del proyecto.

Es importante que la ruta al directorio de trabajo no contenga espacios en blanco, ya que puede ocasionar problemas con la ejecución del sistema de compilación.

Tenga en cuenta que el repositorio ClickHouse utiliza `submodules`. That is what the references to additional repositories are called (i.e. external libraries on which the project depends). It means that when cloning the repository you need to specify the `--recursive` como en el ejemplo anterior. Si el repositorio se ha clonado sin submódulos, para descargarlos debe ejecutar lo siguiente:

    git submodule init
    git submodule update

Puede verificar el estado con el comando: `git submodule status`.

Si recibe el siguiente mensaje de error:

    Permission denied (publickey).
    fatal: Could not read from remote repository.

    Please make sure you have the correct access rights
    and the repository exists.

Por lo general, significa que faltan las claves SSH para conectarse a GitHub. Estas teclas se encuentran normalmente en `~/.ssh`. Para que las claves SSH sean aceptadas, debe cargarlas en la sección de configuración de la interfaz de usuario de GitHub.

También puede clonar el repositorio a través del protocolo https:

    git clone https://github.com/ClickHouse/ClickHouse.git

Sin embargo, esto no le permitirá enviar los cambios al servidor. Aún puede usarlo temporalmente y agregar las claves SSH más tarde reemplazando la dirección remota del repositorio con `git remote` comando.

También puede agregar la dirección original del repositorio de ClickHouse a su repositorio local para extraer actualizaciones desde allí:

    git remote add upstream git@github.com:ClickHouse/ClickHouse.git

Después de ejecutar con éxito este comando, podrá extraer actualizaciones del repositorio principal de ClickHouse ejecutando `git pull upstream master`.

## Trabajar con submódulos {#working-with-submodules}

Trabajar con submódulos en git podría ser doloroso. Los siguientes comandos ayudarán a administrarlo:

    # ! each command accepts --recursive
    # Update remote URLs for submodules. Barely rare case
    git submodule sync
    # Add new submodules
    git submodule init
    # Update existing submodules to the current state
    git submodule update
    # Two last commands could be merged together
    git submodule update --init

Los siguientes comandos le ayudarían a restablecer todos los submódulos al estado inicial (!¡ADVERTENCIA! - cualquier cambio en el interior será eliminado):

    # Synchronizes submodules' remote URL with .gitmodules
    git submodule sync --recursive
    # Update the registered submodules with initialize not yet initialized
    git submodule update --init --recursive
    # Reset all changes done after HEAD
    git submodule foreach git reset --hard
    # Clean files from .gitignore
    git submodule foreach git clean -xfd
    # Repeat last 4 commands for all submodule
    git submodule foreach git submodule sync --recursive
    git submodule foreach git submodule update --init --recursive
    git submodule foreach git submodule foreach git reset --hard
    git submodule foreach git submodule foreach git clean -xfd

# Sistema de construcción {#build-system}

ClickHouse utiliza CMake y Ninja para la construcción.

CMake - un sistema de meta-construcción que puede generar archivos Ninja (tareas de construcción).
Ninja: un sistema de compilación más pequeño con un enfoque en la velocidad utilizada para ejecutar esas tareas generadas por cmake.

Para instalar en Ubuntu, Debian o Mint run `sudo apt install cmake ninja-build`.

En CentOS, RedHat se ejecuta `sudo yum install cmake ninja-build`.

Si usa Arch o Gentoo, probablemente lo sepa usted mismo cómo instalar CMake.

Para instalar CMake y Ninja en Mac OS X, primero instale Homebrew y luego instale todo lo demás a través de brew:

    /usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
    brew install cmake ninja

A continuación, verifique la versión de CMake: `cmake --version`. Si está por debajo de 3.3, debe instalar una versión más reciente desde el sitio web: https://cmake.org/download/.

# Bibliotecas externas opcionales {#optional-external-libraries}

ClickHouse utiliza varias bibliotecas externas para la construcción. Todos ellos no necesitan ser instalados por separado, ya que se construyen junto con ClickHouse a partir de las fuentes ubicadas en los submódulos. Puede consultar la lista en `contrib`.

# Compilador de C ++ {#c-compiler}

Los compiladores GCC a partir de la versión 9 y Clang versión 8 o superior son compatibles para construir ClickHouse.

Las compilaciones oficiales de Yandex actualmente usan GCC porque genera código de máquina de un rendimiento ligeramente mejor (con una diferencia de hasta varios por ciento según nuestros puntos de referencia). Y Clang es más conveniente para el desarrollo generalmente. Sin embargo, nuestra plataforma de integración continua (CI) ejecuta verificaciones de aproximadamente una docena de combinaciones de compilación.

Para instalar GCC en Ubuntu, ejecute: `sudo apt install gcc g++`

Compruebe la versión de gcc: `gcc --version`. Si está por debajo de 9, siga las instrucciones aquí: https://clickhouse.tech/docs/es/development/build/#install-gcc-9.

La compilación de Mac OS X solo es compatible con Clang. Sólo tiene que ejecutar `brew install llvm`

Si decide utilizar Clang, también puede instalar `libc++` y `lld` si usted sabe lo que es. Utilizar `ccache` también se recomienda.

# El proceso de construcción {#the-building-process}

Ahora que está listo para construir ClickHouse, le recomendamos que cree un directorio separado `build` dentro `ClickHouse` que contendrá todos los de la generación de artefactos:

    mkdir build
    cd build

Puede tener varios directorios diferentes (build\_release, build\_debug, etc.) para diferentes tipos de construcción.

Mientras que dentro de la `build` directorio, configure su compilación ejecutando CMake. Antes de la primera ejecución, debe definir variables de entorno que especifiquen el compilador (compilador gcc versión 9 en este ejemplo).

Linux:

    export CC=gcc-9 CXX=g++-9
    cmake ..

Mac OS X:

    export CC=clang CXX=clang++
    cmake ..

El `CC` variable especifica el compilador para C (abreviatura de C Compiler), y `CXX` variable indica qué compilador de C ++ se usará para compilar.

Para una construcción más rápida, puede recurrir al `debug` tipo de compilación: una compilación sin optimizaciones. Para ese suministro el siguiente parámetro `-D CMAKE_BUILD_TYPE=Debug`:

    cmake -D CMAKE_BUILD_TYPE=Debug ..

Puede cambiar el tipo de compilación ejecutando este comando en el `build` directorio.

Ejecutar ninja para construir:

    ninja clickhouse-server clickhouse-client

Solo los binarios requeridos se van a construir en este ejemplo.

Si necesita construir todos los binarios (utilidades y pruebas), debe ejecutar ninja sin parámetros:

    ninja

La compilación completa requiere aproximadamente 30 GB de espacio libre en disco o 15 GB para construir los binarios principales.

Cuando hay una gran cantidad de RAM disponible en la máquina de compilación, debe limitar el número de tareas de compilación que se ejecutan en paralelo con `-j` parámetro:

    ninja -j 1 clickhouse-server clickhouse-client

En máquinas con 4GB de RAM, se recomienda especificar 1, para 8GB de RAM `-j 2` se recomienda.

Si recibe el mensaje: `ninja: error: loading 'build.ninja': No such file or directory`, significa que la generación de una configuración de compilación ha fallado y necesita inspeccionar el mensaje anterior.

Cuando se inicie correctamente el proceso de construcción, verá el progreso de la compilación: el número de tareas procesadas y el número total de tareas.

Al crear mensajes sobre archivos protobuf en la biblioteca libhdfs2, como `libprotobuf WARNING` puede aparecer. Afectan a nada y son seguros para ser ignorado.

Tras la compilación exitosa, obtienes un archivo ejecutable `ClickHouse/<build_dir>/programs/clickhouse`:

    ls -l programs/clickhouse

# Ejecución del ejecutable construido de ClickHouse {#running-the-built-executable-of-clickhouse}

Para ejecutar el servidor bajo el usuario actual, debe navegar hasta `ClickHouse/programs/server/` (situado fuera de `build`) y ejecutar:

    ../../build/programs/clickhouse server

En este caso, ClickHouse usará archivos de configuración ubicados en el directorio actual. Puede ejecutar `clickhouse server` desde cualquier directorio que especifique la ruta a un archivo de configuración como un parámetro de línea de comandos `--config-file`.

Para conectarse a ClickHouse con clickhouse-client en otro terminal, vaya a `ClickHouse/build/programs/` y ejecutar `clickhouse client`.

Si usted consigue `Connection refused` mensaje en Mac OS X o FreeBSD, intente especificar la dirección de host 127.0.0.1:

    clickhouse client --host 127.0.0.1

Puede reemplazar la versión de producción del binario ClickHouse instalado en su sistema con su binario ClickHouse personalizado. Para ello, instale ClickHouse en su máquina siguiendo las instrucciones del sitio web oficial. A continuación, ejecute lo siguiente:

    sudo service clickhouse-server stop
    sudo cp ClickHouse/build/programs/clickhouse /usr/bin/
    sudo service clickhouse-server start

Tenga en cuenta que `clickhouse-client`, `clickhouse-server` y otros son enlaces simbólicos a los comúnmente compartidos `clickhouse` binario.

También puede ejecutar su binario ClickHouse personalizado con el archivo de configuración del paquete ClickHouse instalado en su sistema:

    sudo service clickhouse-server stop
    sudo -u clickhouse ClickHouse/build/programs/clickhouse server --config-file /etc/clickhouse-server/config.xml

# IDE (entorno de desarrollo integrado) {#ide-integrated-development-environment}

Si no sabe qué IDE usar, le recomendamos que use CLion. CLion es un software comercial, pero ofrece un período de prueba gratuito de 30 días. También es gratuito para los estudiantes. CLion se puede usar tanto en Linux como en Mac OS X.

KDevelop y QTCreator son otras excelentes alternativas de un IDE para desarrollar ClickHouse. KDevelop viene como un IDE muy útil aunque inestable. Si KDevelop se bloquea después de un tiempo al abrir el proyecto, debe hacer clic “Stop All” botón tan pronto como se ha abierto la lista de archivos del proyecto. Después de hacerlo, KDevelop debería estar bien para trabajar.

Como editores de código simples, puede usar Sublime Text o Visual Studio Code, o Kate (todos los cuales están disponibles en Linux).

Por si acaso, vale la pena mencionar que CLion crea `build` por sí mismo, también por sí mismo selecciona `debug` para el tipo de compilación, para la configuración usa una versión de CMake que está definida en CLion y no la instalada por usted, y finalmente, CLion usará `make` para ejecutar tareas de compilación en lugar de `ninja`. Este es un comportamiento normal, solo tenlo en cuenta para evitar confusiones.

# Código de escritura {#writing-code}

La descripción de la arquitectura ClickHouse se puede encontrar aquí: https://clickhouse.tech/docs/es/desarrollo/arquitectura/

La Guía de estilo de código: https://clickhouse.tech/docs/en/development/style/

Pruebas de escritura: https://clickhouse.tech/docs/en/development/tests/

Lista de tareas: https://github.com/ClickHouse/ClickHouse/contribute

# Datos de prueba {#test-data}

El desarrollo de ClickHouse a menudo requiere cargar conjuntos de datos realistas. Es particularmente importante para las pruebas de rendimiento. Tenemos un conjunto especialmente preparado de datos anónimos de Yandex.Métrica. Se requiere, además, unos 3 GB de espacio libre en disco. Tenga en cuenta que estos datos no son necesarios para realizar la mayoría de las tareas de desarrollo.

    sudo apt install wget xz-utils

    wget https://clickhouse-datasets.s3.yandex.net/hits/tsv/hits_v1.tsv.xz
    wget https://clickhouse-datasets.s3.yandex.net/visits/tsv/visits_v1.tsv.xz

    xz -v -d hits_v1.tsv.xz
    xz -v -d visits_v1.tsv.xz

    clickhouse-client

    CREATE DATABASE IF NOT EXISTS test

    CREATE TABLE test.hits ( WatchID UInt64,  JavaEnable UInt8,  Title String,  GoodEvent Int16,  EventTime DateTime,  EventDate Date,  CounterID UInt32,  ClientIP UInt32,  ClientIP6 FixedString(16),  RegionID UInt32,  UserID UInt64,  CounterClass Int8,  OS UInt8,  UserAgent UInt8,  URL String,  Referer String,  URLDomain String,  RefererDomain String,  Refresh UInt8,  IsRobot UInt8,  RefererCategories Array(UInt16),  URLCategories Array(UInt16),  URLRegions Array(UInt32),  RefererRegions Array(UInt32),  ResolutionWidth UInt16,  ResolutionHeight UInt16,  ResolutionDepth UInt8,  FlashMajor UInt8,  FlashMinor UInt8,  FlashMinor2 String,  NetMajor UInt8,  NetMinor UInt8,  UserAgentMajor UInt16,  UserAgentMinor FixedString(2),  CookieEnable UInt8,  JavascriptEnable UInt8,  IsMobile UInt8,  MobilePhone UInt8,  MobilePhoneModel String,  Params String,  IPNetworkID UInt32,  TraficSourceID Int8,  SearchEngineID UInt16,  SearchPhrase String,  AdvEngineID UInt8,  IsArtifical UInt8,  WindowClientWidth UInt16,  WindowClientHeight UInt16,  ClientTimeZone Int16,  ClientEventTime DateTime,  SilverlightVersion1 UInt8,  SilverlightVersion2 UInt8,  SilverlightVersion3 UInt32,  SilverlightVersion4 UInt16,  PageCharset String,  CodeVersion UInt32,  IsLink UInt8,  IsDownload UInt8,  IsNotBounce UInt8,  FUniqID UInt64,  HID UInt32,  IsOldCounter UInt8,  IsEvent UInt8,  IsParameter UInt8,  DontCountHits UInt8,  WithHash UInt8,  HitColor FixedString(1),  UTCEventTime DateTime,  Age UInt8,  Sex UInt8,  Income UInt8,  Interests UInt16,  Robotness UInt8,  GeneralInterests Array(UInt16),  RemoteIP UInt32,  RemoteIP6 FixedString(16),  WindowName Int32,  OpenerName Int32,  HistoryLength Int16,  BrowserLanguage FixedString(2),  BrowserCountry FixedString(2),  SocialNetwork String,  SocialAction String,  HTTPError UInt16,  SendTiming Int32,  DNSTiming Int32,  ConnectTiming Int32,  ResponseStartTiming Int32,  ResponseEndTiming Int32,  FetchTiming Int32,  RedirectTiming Int32,  DOMInteractiveTiming Int32,  DOMContentLoadedTiming Int32,  DOMCompleteTiming Int32,  LoadEventStartTiming Int32,  LoadEventEndTiming Int32,  NSToDOMContentLoadedTiming Int32,  FirstPaintTiming Int32,  RedirectCount Int8,  SocialSourceNetworkID UInt8,  SocialSourcePage String,  ParamPrice Int64,  ParamOrderID String,  ParamCurrency FixedString(3),  ParamCurrencyID UInt16,  GoalsReached Array(UInt32),  OpenstatServiceName String,  OpenstatCampaignID String,  OpenstatAdID String,  OpenstatSourceID String,  UTMSource String,  UTMMedium String,  UTMCampaign String,  UTMContent String,  UTMTerm String,  FromTag String,  HasGCLID UInt8,  RefererHash UInt64,  URLHash UInt64,  CLID UInt32,  YCLID UInt64,  ShareService String,  ShareURL String,  ShareTitle String,  `ParsedParams.Key1` Array(String),  `ParsedParams.Key2` Array(String),  `ParsedParams.Key3` Array(String),  `ParsedParams.Key4` Array(String),  `ParsedParams.Key5` Array(String),  `ParsedParams.ValueDouble` Array(Float64),  IslandID FixedString(16),  RequestNum UInt32,  RequestTry UInt8) ENGINE = MergeTree PARTITION BY toYYYYMM(EventDate) SAMPLE BY intHash32(UserID) ORDER BY (CounterID, EventDate, intHash32(UserID), EventTime);

    CREATE TABLE test.visits ( CounterID UInt32,  StartDate Date,  Sign Int8,  IsNew UInt8,  VisitID UInt64,  UserID UInt64,  StartTime DateTime,  Duration UInt32,  UTCStartTime DateTime,  PageViews Int32,  Hits Int32,  IsBounce UInt8,  Referer String,  StartURL String,  RefererDomain String,  StartURLDomain String,  EndURL String,  LinkURL String,  IsDownload UInt8,  TraficSourceID Int8,  SearchEngineID UInt16,  SearchPhrase String,  AdvEngineID UInt8,  PlaceID Int32,  RefererCategories Array(UInt16),  URLCategories Array(UInt16),  URLRegions Array(UInt32),  RefererRegions Array(UInt32),  IsYandex UInt8,  GoalReachesDepth Int32,  GoalReachesURL Int32,  GoalReachesAny Int32,  SocialSourceNetworkID UInt8,  SocialSourcePage String,  MobilePhoneModel String,  ClientEventTime DateTime,  RegionID UInt32,  ClientIP UInt32,  ClientIP6 FixedString(16),  RemoteIP UInt32,  RemoteIP6 FixedString(16),  IPNetworkID UInt32,  SilverlightVersion3 UInt32,  CodeVersion UInt32,  ResolutionWidth UInt16,  ResolutionHeight UInt16,  UserAgentMajor UInt16,  UserAgentMinor UInt16,  WindowClientWidth UInt16,  WindowClientHeight UInt16,  SilverlightVersion2 UInt8,  SilverlightVersion4 UInt16,  FlashVersion3 UInt16,  FlashVersion4 UInt16,  ClientTimeZone Int16,  OS UInt8,  UserAgent UInt8,  ResolutionDepth UInt8,  FlashMajor UInt8,  FlashMinor UInt8,  NetMajor UInt8,  NetMinor UInt8,  MobilePhone UInt8,  SilverlightVersion1 UInt8,  Age UInt8,  Sex UInt8,  Income UInt8,  JavaEnable UInt8,  CookieEnable UInt8,  JavascriptEnable UInt8,  IsMobile UInt8,  BrowserLanguage UInt16,  BrowserCountry UInt16,  Interests UInt16,  Robotness UInt8,  GeneralInterests Array(UInt16),  Params Array(String),  `Goals.ID` Array(UInt32),  `Goals.Serial` Array(UInt32),  `Goals.EventTime` Array(DateTime),  `Goals.Price` Array(Int64),  `Goals.OrderID` Array(String),  `Goals.CurrencyID` Array(UInt32),  WatchIDs Array(UInt64),  ParamSumPrice Int64,  ParamCurrency FixedString(3),  ParamCurrencyID UInt16,  ClickLogID UInt64,  ClickEventID Int32,  ClickGoodEvent Int32,  ClickEventTime DateTime,  ClickPriorityID Int32,  ClickPhraseID Int32,  ClickPageID Int32,  ClickPlaceID Int32,  ClickTypeID Int32,  ClickResourceID Int32,  ClickCost UInt32,  ClickClientIP UInt32,  ClickDomainID UInt32,  ClickURL String,  ClickAttempt UInt8,  ClickOrderID UInt32,  ClickBannerID UInt32,  ClickMarketCategoryID UInt32,  ClickMarketPP UInt32,  ClickMarketCategoryName String,  ClickMarketPPName String,  ClickAWAPSCampaignName String,  ClickPageName String,  ClickTargetType UInt16,  ClickTargetPhraseID UInt64,  ClickContextType UInt8,  ClickSelectType Int8,  ClickOptions String,  ClickGroupBannerID Int32,  OpenstatServiceName String,  OpenstatCampaignID String,  OpenstatAdID String,  OpenstatSourceID String,  UTMSource String,  UTMMedium String,  UTMCampaign String,  UTMContent String,  UTMTerm String,  FromTag String,  HasGCLID UInt8,  FirstVisit DateTime,  PredLastVisit Date,  LastVisit Date,  TotalVisits UInt32,  `TraficSource.ID` Array(Int8),  `TraficSource.SearchEngineID` Array(UInt16),  `TraficSource.AdvEngineID` Array(UInt8),  `TraficSource.PlaceID` Array(UInt16),  `TraficSource.SocialSourceNetworkID` Array(UInt8),  `TraficSource.Domain` Array(String),  `TraficSource.SearchPhrase` Array(String),  `TraficSource.SocialSourcePage` Array(String),  Attendance FixedString(16),  CLID UInt32,  YCLID UInt64,  NormalizedRefererHash UInt64,  SearchPhraseHash UInt64,  RefererDomainHash UInt64,  NormalizedStartURLHash UInt64,  StartURLDomainHash UInt64,  NormalizedEndURLHash UInt64,  TopLevelDomain UInt64,  URLScheme UInt64,  OpenstatServiceNameHash UInt64,  OpenstatCampaignIDHash UInt64,  OpenstatAdIDHash UInt64,  OpenstatSourceIDHash UInt64,  UTMSourceHash UInt64,  UTMMediumHash UInt64,  UTMCampaignHash UInt64,  UTMContentHash UInt64,  UTMTermHash UInt64,  FromHash UInt64,  WebVisorEnabled UInt8,  WebVisorActivity UInt32,  `ParsedParams.Key1` Array(String),  `ParsedParams.Key2` Array(String),  `ParsedParams.Key3` Array(String),  `ParsedParams.Key4` Array(String),  `ParsedParams.Key5` Array(String),  `ParsedParams.ValueDouble` Array(Float64),  `Market.Type` Array(UInt8),  `Market.GoalID` Array(UInt32),  `Market.OrderID` Array(String),  `Market.OrderPrice` Array(Int64),  `Market.PP` Array(UInt32),  `Market.DirectPlaceID` Array(UInt32),  `Market.DirectOrderID` Array(UInt32),  `Market.DirectBannerID` Array(UInt32),  `Market.GoodID` Array(String),  `Market.GoodName` Array(String),  `Market.GoodQuantity` Array(Int32),  `Market.GoodPrice` Array(Int64),  IslandID FixedString(16)) ENGINE = CollapsingMergeTree(Sign) PARTITION BY toYYYYMM(StartDate) SAMPLE BY intHash32(UserID) ORDER BY (CounterID, StartDate, intHash32(UserID), VisitID);

    clickhouse-client --max_insert_block_size 100000 --query "INSERT INTO test.hits FORMAT TSV" < hits_v1.tsv
    clickhouse-client --max_insert_block_size 100000 --query "INSERT INTO test.visits FORMAT TSV" < visits_v1.tsv

# Creación de solicitud de extracción {#creating-pull-request}

Navega a tu repositorio de fork en la interfaz de usuario de GitHub. Si ha estado desarrollando en una sucursal, debe seleccionar esa sucursal. Habrá un “Pull request” botón situado en la pantalla. En esencia, esto significa “create a request for accepting my changes into the main repository”.

Se puede crear una solicitud de extracción incluso si el trabajo aún no se ha completado. En este caso, por favor ponga la palabra “WIP” (trabajo en curso) al comienzo del título, se puede cambiar más tarde. Esto es útil para la revisión cooperativa y la discusión de los cambios, así como para ejecutar todas las pruebas disponibles. Es importante que proporcione una breve descripción de sus cambios, que más tarde se utilizará para generar registros de cambios de lanzamiento.

Las pruebas comenzarán tan pronto como los empleados de Yandex etiqueten su PR con una etiqueta “can be tested”. The results of some first checks (e.g. code style) will come in within several minutes. Build check results will arrive within half an hour. And the main set of tests will report itself within an hour.

El sistema preparará compilaciones binarias ClickHouse para su solicitud de extracción individualmente. Para recuperar estas compilaciones, haga clic en “Details” junto al link “ClickHouse build check” en la lista de cheques. Allí encontrará enlaces directos a la construcción.deb paquetes de ClickHouse que puede implementar incluso en sus servidores de producción (si no tiene miedo).

Lo más probable es que algunas de las compilaciones fallen las primeras veces. Esto se debe al hecho de que verificamos las compilaciones tanto con gcc como con clang, con casi todas las advertencias existentes (siempre con el `-Werror` bandera) habilitado para sonido. En esa misma página, puede encontrar todos los registros de compilación para que no tenga que compilar ClickHouse de todas las formas posibles.
