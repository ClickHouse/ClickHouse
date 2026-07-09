---
description: 'Requisitos previos e instrucciones de configuración para el desarrollo de ClickHouse'
sidebar_label: 'Requisitos previos'
sidebar_position: 5
slug: /development/developer-instruction
title: 'Requisitos previos para desarrolladores'
doc_type: 'guide'
---

ClickHouse se puede compilar en Linux, FreeBSD y macOS.
Si usa Windows, aun así puede compilar ClickHouse en una máquina virtual que ejecute Linux, por ejemplo, [VirtualBox](https://www.virtualbox.org/) con Ubuntu.

<div id="create-a-repository-on-github">
  ## Crea un repositorio en GitHub
</div>

Para empezar a desarrollar para ClickHouse, necesitarás una cuenta de [GitHub](https://www.github.com/).
Genera también una clave SSH localmente (si todavía no tienes una) y sube la clave pública a GitHub, ya que este es un requisito previo para contribuir parches.

A continuación, haz un fork del [repositorio de ClickHouse](https://github.com/ClickHouse/ClickHouse/) en tu cuenta personal haciendo clic en el botón &quot;fork&quot; de la esquina superior derecha.

Para contribuir cambios, por ejemplo, una corrección para un issue o una funcionalidad, primero haz commit de tus cambios en una rama de tu fork y, después, crea un &quot;Pull Request&quot; con los cambios al repositorio principal.

Para trabajar con repositorios Git, instala Git. Por ejemplo, en Ubuntu, ejecuta:

```sh
sudo apt update
sudo apt install git
```

Puedes encontrar una chuleta de Git [aquí](https://education.github.com/git-cheat-sheet-education.pdf).
Un manual detallado de Git está disponible [aquí](https://git-scm.com/book/en/v2).

<div id="clone-the-repository-to-your-development-machine">
  ## Clona el repositorio en tu equipo de desarrollo
</div>

Primero, descarga los archivos fuente en tu equipo de trabajo; es decir, clona el repositorio:

```sh
git clone git@github.com:your_github_username/ClickHouse.git  # replace the placeholder with your GitHub user name
cd ClickHouse
```

Este comando crea un directorio `ClickHouse/` que contiene el código fuente, las pruebas y otros archivos.
Puede especificar un directorio personalizado para la extracción después de la URL, pero es importante que esta ruta no contenga espacios en blanco, ya que eso podría romper la compilación más adelante.

El repositorio Git de ClickHouse usa submódulos para incorporar bibliotecas de terceros.
Los submódulos no se extraen de forma predeterminada.
Puede hacer una de las siguientes cosas:

* ejecutar `git clone` con la opción `--recurse-submodules`,

* si ejecuta `git clone` sin `--recurse-submodules`, ejecute `git submodule update --init --jobs <N>` para extraer explícitamente todos los submódulos. (`<N>` puede establecerse, por ejemplo, en `12` para paralelizar la descarga).

* si ejecuta `git clone` sin `--recurse-submodules` y quiere usar una extracción superficial ([shallow](https://github.blog/2020-12-21-get-up-to-speed-with-partial-clone-and-shallow-clone/)) de los submódulos para omitir su historial y ahorrar algo de espacio, ejecute `./contrib/update-submodules.sh`. CI usa esta alternativa, pero no se recomienda para el desarrollo local, ya que hace que trabajar con submódulos sea menos cómodo y más lento.

Para comprobar el estado de los submódulos de Git, ejecute `git submodule status`.

Si obtiene el siguiente mensaje de error

```bash
Permission denied (publickey).
fatal: Could not read from remote repository.

Please make sure you have the correct access rights
and the repository exists.
```

faltan las claves SSH necesarias para conectarse a GitHub.
Estas claves suelen estar en `~/.ssh`.
Para que GitHub acepte las claves SSH, tienes que cargarlas en su configuración.

También puedes clonar el repositorio mediante HTTPS:

```sh
git clone https://github.com/ClickHouse/ClickHouse.git
```

Sin embargo, esto no te permitirá enviar tus cambios al servidor.
Aun así, puedes usarlo temporalmente y añadir la clave SSH más adelante sustituyendo la dirección remota del repositorio con el comando `git remote`.

También puedes añadir la dirección original del repositorio de ClickHouse a tu repositorio local para obtener actualizaciones desde allí:

```sh
git remote add upstream git@github.com:ClickHouse/ClickHouse.git
```

Después de ejecutar correctamente este comando, podrás traer actualizaciones del repositorio principal de ClickHouse ejecutando `git pull upstream master`.

:::tip
Por favor, no uses `git push` sin más; podrías hacer push al remoto equivocado o a la rama equivocada.
Es mejor especificar explícitamente los nombres del remoto y de la rama; por ejemplo, `git push origin my_branch_name`.
:::

<div id="writing-code">
  ## Escribir código
</div>

A continuación se incluyen algunos enlaces rápidos que pueden ser útiles al escribir código para ClickHouse:

* [Arquitectura de ClickHouse](/es/development/architecture/).
* [Guía de estilo del código](/es/development/style/).
* [Bibliotecas de terceros](/es/development/contrib#adding-and-maintaining-third-party-libraries)
* [Cómo escribir pruebas](/es/development/tests/)
* [Issues abiertas](https://github.com/ClickHouse/ClickHouse/issues?q=is%3Aopen+is%3Aissue+label%3A%22easy+task%22)

<div id="ide">
  ### IDE
</div>

[Visual Studio Code](https://code.visualstudio.com/) y [Neovim](https://neovim.io/) son dos opciones que en el pasado han dado buenos resultados para desarrollar ClickHouse. Si usas VS Code, te recomendamos la [extensión clangd](https://marketplace.visualstudio.com/items?itemName=llvm-vs-code-extensions.vscode-clangd) para sustituir IntelliSense, ya que ofrece mucho mejor rendimiento.

[CLion](https://www.jetbrains.com/clion/) es otra gran alternativa. Sin embargo, puede ser más lento en proyectos grandes como ClickHouse. Algunas cosas que debes tener en cuenta al usar CLion:

* CLion crea por su cuenta un directorio `build` y selecciona automáticamente `debug` como tipo de compilación
* Usa una versión de CMake definida en CLion y no la que tienes instalada
* CLion usará `make` para ejecutar tareas de compilación en lugar de `ninja` (este es el comportamiento normal)

Otros IDEs que puedes usar son [Sublime Text](https://www.sublimetext.com/), [Qt Creator](https://www.qt.io/product/development-tools) o [Kate](https://kate-editor.org/).

<div id="create-a-pull-request">
  ## Crear un pull request
</div>

Ve a tu repositorio fork en la interfaz de usuario de GitHub.
Si has estado desarrollando en una rama, debes seleccionar esa rama.
Verás un botón &quot;Pull request&quot; en la pantalla.
En esencia, esto significa &quot;crear una solicitud para aceptar mis cambios en el repositorio principal&quot;.

Se puede crear un pull request incluso si el trabajo todavía no está terminado.
En este caso, pon la palabra &quot;WIP&quot; (trabajo en curso) al principio del título; más adelante se puede cambiar.
Esto resulta útil para la revisión colaborativa y la discusión de cambios, así como para ejecutar todas las pruebas disponibles.
Es importante que proporciones una breve descripción de tus cambios; más adelante se utilizará para generar el changelog de la versión.

Las pruebas comenzarán en cuanto los empleados de ClickHouse etiqueten tu PR con la etiqueta &quot;can be tested&quot;.
Los resultados de algunas primeras comprobaciones (p. ej. estilo de código) llegarán en unos minutos.
Los resultados de las comprobaciones de compilación llegarán en media hora.
El conjunto principal de pruebas informará de sus resultados en el plazo de una hora.

El sistema preparará compilaciones binarias de ClickHouse específicamente para tu pull request.
Para obtener estas compilaciones, haz clic en el enlace &quot;Details&quot; junto a la entrada &quot;Builds&quot; en la lista de comprobaciones.
Allí encontrarás enlaces directos a los paquetes .deb compilados de ClickHouse que puedes desplegar incluso en tus servidores de producción (si no tienes miedo).

<div id="write-documentation">
  ## Escribir documentación
</div>

Todo pull request que añada una nueva funcionalidad debe ir acompañado de la documentación correspondiente.
Si desea obtener una vista previa de los cambios en la documentación, puede consultar en el archivo README.md las instrucciones para compilar localmente la página de documentación [aquí](https://github.com/ClickHouse/clickhouse-docs).
Al añadir una nueva función a ClickHouse, puede usar la siguiente plantilla como guía:

````markdown
# newFunctionName

A short description of the function goes here. It should describe briefly what it does and a typical usage case.

**Syntax**

\```sql
newFunctionName(arg1, arg2[, arg3])
\```

**Arguments**

- `arg1` — Description of the argument. [DataType](../data-types/float.md)
- `arg2` — Description of the argument. [DataType](../data-types/float.md)
- `arg3` — Description of optional argument (optional). [DataType](../data-types/float.md)

**Implementation Details**

A description of implementation details if relevant.

**Returned value**

- Returns {insert what the function returns here}. [DataType](../data-types/float.md)

**Example**

\```sql title="Query"
SELECT 'write your example query here';
\```

\```response title="Response"
┌───────────────────────────────────┐
│ the result of the query           │
└───────────────────────────────────┘
\```
````

<div id="using-test-data">
  ## Uso de datos de prueba
</div>

El desarrollo de ClickHouse suele requerir la carga de conjuntos de datos realistas.
Esto es especialmente importante para las pruebas de rendimiento.
Contamos con un conjunto de datos anonimizados de analítica web especialmente preparado.
Además, requiere unos 3 GB más de espacio libre en disco.

```sh
    sudo apt install wget xz-utils

    wget https://datasets.clickhouse.com/hits/tsv/hits_v1.tsv.xz
    wget https://datasets.clickhouse.com/visits/tsv/visits_v1.tsv.xz

    xz -v -d hits_v1.tsv.xz
    xz -v -d visits_v1.tsv.xz

    clickhouse-client
```

En clickhouse-client:

```sql
CREATE DATABASE IF NOT EXISTS test;

CREATE TABLE test.hits ( WatchID UInt64,  JavaEnable UInt8,  Title String,  GoodEvent Int16,  EventTime DateTime,  EventDate Date,  CounterID UInt32,  ClientIP UInt32,  ClientIP6 FixedString(16),  RegionID UInt32,  UserID UInt64,  CounterClass Int8,  OS UInt8,  UserAgent UInt8,  URL String,  Referer String,  URLDomain String,  RefererDomain String,  Refresh UInt8,  IsRobot UInt8,  RefererCategories Array(UInt16),  URLCategories Array(UInt16),  URLRegions Array(UInt32),  RefererRegions Array(UInt32),  ResolutionWidth UInt16,  ResolutionHeight UInt16,  ResolutionDepth UInt8,  FlashMajor UInt8,  FlashMinor UInt8,  FlashMinor2 String,  NetMajor UInt8,  NetMinor UInt8,  UserAgentMajor UInt16,  UserAgentMinor FixedString(2),  CookieEnable UInt8,  JavascriptEnable UInt8,  IsMobile UInt8,  MobilePhone UInt8,  MobilePhoneModel String,  Params String,  IPNetworkID UInt32,  TraficSourceID Int8,  SearchEngineID UInt16,  SearchPhrase String,  AdvEngineID UInt8,  IsArtifical UInt8,  WindowClientWidth UInt16,  WindowClientHeight UInt16,  ClientTimeZone Int16,  ClientEventTime DateTime,  SilverlightVersion1 UInt8,  SilverlightVersion2 UInt8,  SilverlightVersion3 UInt32,  SilverlightVersion4 UInt16,  PageCharset String,  CodeVersion UInt32,  IsLink UInt8,  IsDownload UInt8,  IsNotBounce UInt8,  FUniqID UInt64,  HID UInt32,  IsOldCounter UInt8,  IsEvent UInt8,  IsParameter UInt8,  DontCountHits UInt8,  WithHash UInt8,  HitColor FixedString(1),  UTCEventTime DateTime,  Age UInt8,  Sex UInt8,  Income UInt8,  Interests UInt16,  Robotness UInt8,  GeneralInterests Array(UInt16),  RemoteIP UInt32,  RemoteIP6 FixedString(16),  WindowName Int32,  OpenerName Int32,  HistoryLength Int16,  BrowserLanguage FixedString(2),  BrowserCountry FixedString(2),  SocialNetwork String,  SocialAction String,  HTTPError UInt16,  SendTiming Int32,  DNSTiming Int32,  ConnectTiming Int32,  ResponseStartTiming Int32,  ResponseEndTiming Int32,  FetchTiming Int32,  RedirectTiming Int32,  DOMInteractiveTiming Int32,  DOMContentLoadedTiming Int32,  DOMCompleteTiming Int32,  LoadEventStartTiming Int32,  LoadEventEndTiming Int32,  NSToDOMContentLoadedTiming Int32,  FirstPaintTiming Int32,  RedirectCount Int8,  SocialSourceNetworkID UInt8,  SocialSourcePage String,  ParamPrice Int64,  ParamOrderID String,  ParamCurrency FixedString(3),  ParamCurrencyID UInt16,  GoalsReached Array(UInt32),  OpenstatServiceName String,  OpenstatCampaignID String,  OpenstatAdID String,  OpenstatSourceID String,  UTMSource String,  UTMMedium String,  UTMCampaign String,  UTMContent String,  UTMTerm String,  FromTag String,  HasGCLID UInt8,  RefererHash UInt64,  URLHash UInt64,  CLID UInt32,  YCLID UInt64,  ShareService String,  ShareURL String,  ShareTitle String,  `ParsedParams.Key1` Array(String),  `ParsedParams.Key2` Array(String),  `ParsedParams.Key3` Array(String),  `ParsedParams.Key4` Array(String),  `ParsedParams.Key5` Array(String),  `ParsedParams.ValueDouble` Array(Float64),  IslandID FixedString(16),  RequestNum UInt32,  RequestTry UInt8) ENGINE = MergeTree PARTITION BY toYYYYMM(EventDate) SAMPLE BY intHash32(UserID) ORDER BY (CounterID, EventDate, intHash32(UserID), EventTime);

CREATE TABLE test.visits ( CounterID UInt32,  StartDate Date,  Sign Int8,  IsNew UInt8,  VisitID UInt64,  UserID UInt64,  StartTime DateTime,  Duration UInt32,  UTCStartTime DateTime,  PageViews Int32,  Hits Int32,  IsBounce UInt8,  Referer String,  StartURL String,  RefererDomain String,  StartURLDomain String,  EndURL String,  LinkURL String,  IsDownload UInt8,  TraficSourceID Int8,  SearchEngineID UInt16,  SearchPhrase String,  AdvEngineID UInt8,  PlaceID Int32,  RefererCategories Array(UInt16),  URLCategories Array(UInt16),  URLRegions Array(UInt32),  RefererRegions Array(UInt32),  IsYandex UInt8,  GoalReachesDepth Int32,  GoalReachesURL Int32,  GoalReachesAny Int32,  SocialSourceNetworkID UInt8,  SocialSourcePage String,  MobilePhoneModel String,  ClientEventTime DateTime,  RegionID UInt32,  ClientIP UInt32,  ClientIP6 FixedString(16),  RemoteIP UInt32,  RemoteIP6 FixedString(16),  IPNetworkID UInt32,  SilverlightVersion3 UInt32,  CodeVersion UInt32,  ResolutionWidth UInt16,  ResolutionHeight UInt16,  UserAgentMajor UInt16,  UserAgentMinor UInt16,  WindowClientWidth UInt16,  WindowClientHeight UInt16,  SilverlightVersion2 UInt8,  SilverlightVersion4 UInt16,  FlashVersion3 UInt16,  FlashVersion4 UInt16,  ClientTimeZone Int16,  OS UInt8,  UserAgent UInt8,  ResolutionDepth UInt8,  FlashMajor UInt8,  FlashMinor UInt8,  NetMajor UInt8,  NetMinor UInt8,  MobilePhone UInt8,  SilverlightVersion1 UInt8,  Age UInt8,  Sex UInt8,  Income UInt8,  JavaEnable UInt8,  CookieEnable UInt8,  JavascriptEnable UInt8,  IsMobile UInt8,  BrowserLanguage UInt16,  BrowserCountry UInt16,  Interests UInt16,  Robotness UInt8,  GeneralInterests Array(UInt16),  Params Array(String),  `Goals.ID` Array(UInt32),  `Goals.Serial` Array(UInt32),  `Goals.EventTime` Array(DateTime),  `Goals.Price` Array(Int64),  `Goals.OrderID` Array(String),  `Goals.CurrencyID` Array(UInt32),  WatchIDs Array(UInt64),  ParamSumPrice Int64,  ParamCurrency FixedString(3),  ParamCurrencyID UInt16,  ClickLogID UInt64,  ClickEventID Int32,  ClickGoodEvent Int32,  ClickEventTime DateTime,  ClickPriorityID Int32,  ClickPhraseID Int32,  ClickPageID Int32,  ClickPlaceID Int32,  ClickTypeID Int32,  ClickResourceID Int32,  ClickCost UInt32,  ClickClientIP UInt32,  ClickDomainID UInt32,  ClickURL String,  ClickAttempt UInt8,  ClickOrderID UInt32,  ClickBannerID UInt32,  ClickMarketCategoryID UInt32,  ClickMarketPP UInt32,  ClickMarketCategoryName String,  ClickMarketPPName String,  ClickAWAPSCampaignName String,  ClickPageName String,  ClickTargetType UInt16,  ClickTargetPhraseID UInt64,  ClickContextType UInt8,  ClickSelectType Int8,  ClickOptions String,  ClickGroupBannerID Int32,  OpenstatServiceName String,  OpenstatCampaignID String,  OpenstatAdID String,  OpenstatSourceID String,  UTMSource String,  UTMMedium String,  UTMCampaign String,  UTMContent String,  UTMTerm String,  FromTag String,  HasGCLID UInt8,  FirstVisit DateTime,  PredLastVisit Date,  LastVisit Date,  TotalVisits UInt32,  `TraficSource.ID` Array(Int8),  `TraficSource.SearchEngineID` Array(UInt16),  `TraficSource.AdvEngineID` Array(UInt8),  `TraficSource.PlaceID` Array(UInt16),  `TraficSource.SocialSourceNetworkID` Array(UInt8),  `TraficSource.Domain` Array(String),  `TraficSource.SearchPhrase` Array(String),  `TraficSource.SocialSourcePage` Array(String),  Attendance FixedString(16),  CLID UInt32,  YCLID UInt64,  NormalizedRefererHash UInt64,  SearchPhraseHash UInt64,  RefererDomainHash UInt64,  NormalizedStartURLHash UInt64,  StartURLDomainHash UInt64,  NormalizedEndURLHash UInt64,  TopLevelDomain UInt64,  URLScheme UInt64,  OpenstatServiceNameHash UInt64,  OpenstatCampaignIDHash UInt64,  OpenstatAdIDHash UInt64,  OpenstatSourceIDHash UInt64,  UTMSourceHash UInt64,  UTMMediumHash UInt64,  UTMCampaignHash UInt64,  UTMContentHash UInt64,  UTMTermHash UInt64,  FromHash UInt64,  WebVisorEnabled UInt8,  WebVisorActivity UInt32,  `ParsedParams.Key1` Array(String),  `ParsedParams.Key2` Array(String),  `ParsedParams.Key3` Array(String),  `ParsedParams.Key4` Array(String),  `ParsedParams.Key5` Array(String),  `ParsedParams.ValueDouble` Array(Float64),  `Market.Type` Array(UInt8),  `Market.GoalID` Array(UInt32),  `Market.OrderID` Array(String),  `Market.OrderPrice` Array(Int64),  `Market.PP` Array(UInt32),  `Market.DirectPlaceID` Array(UInt32),  `Market.DirectOrderID` Array(UInt32),  `Market.DirectBannerID` Array(UInt32),  `Market.GoodID` Array(String),  `Market.GoodName` Array(String),  `Market.GoodQuantity` Array(Int32),  `Market.GoodPrice` Array(Int64),  IslandID FixedString(16)) ENGINE = CollapsingMergeTree(Sign) PARTITION BY toYYYYMM(StartDate) SAMPLE BY intHash32(UserID) ORDER BY (CounterID, StartDate, intHash32(UserID), VisitID);

```

Importa los datos:

```bash
clickhouse-client --max_insert_block_size 100000 --query "INSERT INTO test.hits FORMAT TSV" < hits_v1.tsv
clickhouse-client --max_insert_block_size 100000 --query "INSERT INTO test.visits FORMAT TSV" < visits_v1.tsv
```