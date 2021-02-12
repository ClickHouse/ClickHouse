---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 12
toc_title: Tutorial
---

# Tutorial de ClickHouse {#clickhouse-tutorial}

## Qué Esperar de Este Tutorial? {#what-to-expect-from-this-tutorial}

Al pasar por este tutorial, aprenderá cómo configurar un clúster de ClickHouse simple. Será pequeño, pero tolerante a fallos y escalable. Luego usaremos uno de los conjuntos de datos de ejemplo para llenarlo con datos y ejecutar algunas consultas de demostración.

## Configuración de nodo único {#single-node-setup}

Para posponer las complejidades de un entorno distribuido, comenzaremos con la implementación de ClickHouse en un único servidor o máquina virtual. ClickHouse generalmente se instala desde [deb](install.md#install-from-deb-packages) o [RPM](install.md#from-rpm-packages) paquetes, pero hay [alternativa](install.md#from-docker-image) para los sistemas operativos que no los admiten.

Por ejemplo, ha elegido `deb` paquetes y ejecutado:

``` bash
{% include 'install/deb.sh' %}
```

¿Qué tenemos en los paquetes que tengo instalados:

-   `clickhouse-client` el paquete contiene [Casa de clics-cliente](../interfaces/cli.md) aplicación, cliente interactivo de la consola ClickHouse.
-   `clickhouse-common` El paquete contiene un archivo ejecutable ClickHouse.
-   `clickhouse-server` El paquete contiene archivos de configuración para ejecutar ClickHouse como servidor.

Los archivos de configuración del servidor se encuentran en `/etc/clickhouse-server/`. Antes de ir más lejos, tenga en cuenta el `<path>` elemento en `config.xml`. La ruta determina la ubicación para el almacenamiento de datos, por lo que debe ubicarse en un volumen con gran capacidad de disco; el valor predeterminado es `/var/lib/clickhouse/`. Si desea ajustar la configuración, no es útil editar directamente `config.xml` archivo, teniendo en cuenta que podría ser reescrito en futuras actualizaciones de paquetes. La forma recomendada de anular los elementos de configuración es crear [archivos en config.directorio d](../operations/configuration-files.md) que sirven como “patches” de configuración.XML.

Como habrás notado, `clickhouse-server` no se inicia automáticamente después de la instalación del paquete. Tampoco se reiniciará automáticamente después de las actualizaciones. La forma en que inicia el servidor depende de su sistema de inicio, por lo general, es:

``` bash
sudo service clickhouse-server start
```

o

``` bash
sudo /etc/init.d/clickhouse-server start
```

La ubicación predeterminada para los registros del servidor es `/var/log/clickhouse-server/`. El servidor está listo para manejar las conexiones de cliente una vez que registra el `Ready for connections` mensaje.

Una vez que el `clickhouse-server` está en funcionamiento, podemos usar `clickhouse-client` para conectarse al servidor y ejecutar algunas consultas de prueba como `SELECT "Hello, world!";`.

<details markdown="1">

<summary>Consejos rápidos para clickhouse-cliente</summary>

Modo interactivo:

``` bash
clickhouse-client
clickhouse-client --host=... --port=... --user=... --password=...
```

Habilitar consultas multilínea:

``` bash
clickhouse-client -m
clickhouse-client --multiline
```

Ejecutar consultas en modo por lotes:

``` bash
clickhouse-client --query='SELECT 1'
echo 'SELECT 1' | clickhouse-client
clickhouse-client <<< 'SELECT 1'
```

Insertar datos de un archivo en el formato especificado:

``` bash
clickhouse-client --query='INSERT INTO table VALUES' < data.txt
clickhouse-client --query='INSERT INTO table FORMAT TabSeparated' < data.tsv
```

</details>

## Importar conjunto de datos de muestra {#import-sample-dataset}

Ahora es el momento de llenar nuestro servidor ClickHouse con algunos datos de muestra. En este tutorial, usaremos los datos anónimos de Yandex.Metrica, el primer servicio que ejecuta ClickHouse en forma de producción antes de que se convirtiera en código abierto (más sobre eso en [sección de historia](../introduction/history.md)). Hay [múltiples formas de importar Yandex.Conjunto de datos de Metrica](example-datasets/metrica.md), y por el bien del tutorial, iremos con el más realista.

### Descargar y extraer datos de tabla {#download-and-extract-table-data}

``` bash
curl https://clickhouse-datasets.s3.yandex.net/hits/tsv/hits_v1.tsv.xz | unxz --threads=`nproc` > hits_v1.tsv
curl https://clickhouse-datasets.s3.yandex.net/visits/tsv/visits_v1.tsv.xz | unxz --threads=`nproc` > visits_v1.tsv
```

Los archivos extraídos tienen un tamaño de aproximadamente 10 GB.

### Crear tablas {#create-tables}

Como en la mayoría de los sistemas de gestión de bases de datos, ClickHouse agrupa lógicamente las tablas en “databases”. Hay un `default` base de datos, pero crearemos una nueva llamada `tutorial`:

``` bash
clickhouse-client --query "CREATE DATABASE IF NOT EXISTS tutorial"
```

La sintaxis para crear tablas es mucho más complicada en comparación con las bases de datos (ver [referencia](../sql-reference/statements/create.md). En general `CREATE TABLE` declaración tiene que especificar tres cosas clave:

1.  Nombre de la tabla que se va a crear.
2.  Table schema, i.e. list of columns and their [tipos de datos](../sql-reference/data-types/index.md).
3.  [Motor de tabla](../engines/table-engines/index.md) y su configuración, que determina todos los detalles sobre cómo se ejecutarán físicamente las consultas a esta tabla.

El Yandex.Metrica es un servicio de análisis web, y el conjunto de datos de muestra no cubre toda su funcionalidad, por lo que solo hay dos tablas para crear:

-   `hits` es una tabla con cada acción realizada por todos los usuarios en todos los sitios web cubiertos por el servicio.
-   `visits` es una tabla que contiene sesiones precompiladas en lugar de acciones individuales.

Veamos y ejecutemos las consultas de tabla de creación real para estas tablas:

``` sql
CREATE TABLE tutorial.hits_v1
(
    `WatchID` UInt64,
    `JavaEnable` UInt8,
    `Title` String,
    `GoodEvent` Int16,
    `EventTime` DateTime,
    `EventDate` Date,
    `CounterID` UInt32,
    `ClientIP` UInt32,
    `ClientIP6` FixedString(16),
    `RegionID` UInt32,
    `UserID` UInt64,
    `CounterClass` Int8,
    `OS` UInt8,
    `UserAgent` UInt8,
    `URL` String,
    `Referer` String,
    `URLDomain` String,
    `RefererDomain` String,
    `Refresh` UInt8,
    `IsRobot` UInt8,
    `RefererCategories` Array(UInt16),
    `URLCategories` Array(UInt16),
    `URLRegions` Array(UInt32),
    `RefererRegions` Array(UInt32),
    `ResolutionWidth` UInt16,
    `ResolutionHeight` UInt16,
    `ResolutionDepth` UInt8,
    `FlashMajor` UInt8,
    `FlashMinor` UInt8,
    `FlashMinor2` String,
    `NetMajor` UInt8,
    `NetMinor` UInt8,
    `UserAgentMajor` UInt16,
    `UserAgentMinor` FixedString(2),
    `CookieEnable` UInt8,
    `JavascriptEnable` UInt8,
    `IsMobile` UInt8,
    `MobilePhone` UInt8,
    `MobilePhoneModel` String,
    `Params` String,
    `IPNetworkID` UInt32,
    `TraficSourceID` Int8,
    `SearchEngineID` UInt16,
    `SearchPhrase` String,
    `AdvEngineID` UInt8,
    `IsArtifical` UInt8,
    `WindowClientWidth` UInt16,
    `WindowClientHeight` UInt16,
    `ClientTimeZone` Int16,
    `ClientEventTime` DateTime,
    `SilverlightVersion1` UInt8,
    `SilverlightVersion2` UInt8,
    `SilverlightVersion3` UInt32,
    `SilverlightVersion4` UInt16,
    `PageCharset` String,
    `CodeVersion` UInt32,
    `IsLink` UInt8,
    `IsDownload` UInt8,
    `IsNotBounce` UInt8,
    `FUniqID` UInt64,
    `HID` UInt32,
    `IsOldCounter` UInt8,
    `IsEvent` UInt8,
    `IsParameter` UInt8,
    `DontCountHits` UInt8,
    `WithHash` UInt8,
    `HitColor` FixedString(1),
    `UTCEventTime` DateTime,
    `Age` UInt8,
    `Sex` UInt8,
    `Income` UInt8,
    `Interests` UInt16,
    `Robotness` UInt8,
    `GeneralInterests` Array(UInt16),
    `RemoteIP` UInt32,
    `RemoteIP6` FixedString(16),
    `WindowName` Int32,
    `OpenerName` Int32,
    `HistoryLength` Int16,
    `BrowserLanguage` FixedString(2),
    `BrowserCountry` FixedString(2),
    `SocialNetwork` String,
    `SocialAction` String,
    `HTTPError` UInt16,
    `SendTiming` Int32,
    `DNSTiming` Int32,
    `ConnectTiming` Int32,
    `ResponseStartTiming` Int32,
    `ResponseEndTiming` Int32,
    `FetchTiming` Int32,
    `RedirectTiming` Int32,
    `DOMInteractiveTiming` Int32,
    `DOMContentLoadedTiming` Int32,
    `DOMCompleteTiming` Int32,
    `LoadEventStartTiming` Int32,
    `LoadEventEndTiming` Int32,
    `NSToDOMContentLoadedTiming` Int32,
    `FirstPaintTiming` Int32,
    `RedirectCount` Int8,
    `SocialSourceNetworkID` UInt8,
    `SocialSourcePage` String,
    `ParamPrice` Int64,
    `ParamOrderID` String,
    `ParamCurrency` FixedString(3),
    `ParamCurrencyID` UInt16,
    `GoalsReached` Array(UInt32),
    `OpenstatServiceName` String,
    `OpenstatCampaignID` String,
    `OpenstatAdID` String,
    `OpenstatSourceID` String,
    `UTMSource` String,
    `UTMMedium` String,
    `UTMCampaign` String,
    `UTMContent` String,
    `UTMTerm` String,
    `FromTag` String,
    `HasGCLID` UInt8,
    `RefererHash` UInt64,
    `URLHash` UInt64,
    `CLID` UInt32,
    `YCLID` UInt64,
    `ShareService` String,
    `ShareURL` String,
    `ShareTitle` String,
    `ParsedParams` Nested(
        Key1 String,
        Key2 String,
        Key3 String,
        Key4 String,
        Key5 String,
        ValueDouble Float64),
    `IslandID` FixedString(16),
    `RequestNum` UInt32,
    `RequestTry` UInt8
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(EventDate)
ORDER BY (CounterID, EventDate, intHash32(UserID))
SAMPLE BY intHash32(UserID)
SETTINGS index_granularity = 8192
```

``` sql
CREATE TABLE tutorial.visits_v1
(
    `CounterID` UInt32,
    `StartDate` Date,
    `Sign` Int8,
    `IsNew` UInt8,
    `VisitID` UInt64,
    `UserID` UInt64,
    `StartTime` DateTime,
    `Duration` UInt32,
    `UTCStartTime` DateTime,
    `PageViews` Int32,
    `Hits` Int32,
    `IsBounce` UInt8,
    `Referer` String,
    `StartURL` String,
    `RefererDomain` String,
    `StartURLDomain` String,
    `EndURL` String,
    `LinkURL` String,
    `IsDownload` UInt8,
    `TraficSourceID` Int8,
    `SearchEngineID` UInt16,
    `SearchPhrase` String,
    `AdvEngineID` UInt8,
    `PlaceID` Int32,
    `RefererCategories` Array(UInt16),
    `URLCategories` Array(UInt16),
    `URLRegions` Array(UInt32),
    `RefererRegions` Array(UInt32),
    `IsYandex` UInt8,
    `GoalReachesDepth` Int32,
    `GoalReachesURL` Int32,
    `GoalReachesAny` Int32,
    `SocialSourceNetworkID` UInt8,
    `SocialSourcePage` String,
    `MobilePhoneModel` String,
    `ClientEventTime` DateTime,
    `RegionID` UInt32,
    `ClientIP` UInt32,
    `ClientIP6` FixedString(16),
    `RemoteIP` UInt32,
    `RemoteIP6` FixedString(16),
    `IPNetworkID` UInt32,
    `SilverlightVersion3` UInt32,
    `CodeVersion` UInt32,
    `ResolutionWidth` UInt16,
    `ResolutionHeight` UInt16,
    `UserAgentMajor` UInt16,
    `UserAgentMinor` UInt16,
    `WindowClientWidth` UInt16,
    `WindowClientHeight` UInt16,
    `SilverlightVersion2` UInt8,
    `SilverlightVersion4` UInt16,
    `FlashVersion3` UInt16,
    `FlashVersion4` UInt16,
    `ClientTimeZone` Int16,
    `OS` UInt8,
    `UserAgent` UInt8,
    `ResolutionDepth` UInt8,
    `FlashMajor` UInt8,
    `FlashMinor` UInt8,
    `NetMajor` UInt8,
    `NetMinor` UInt8,
    `MobilePhone` UInt8,
    `SilverlightVersion1` UInt8,
    `Age` UInt8,
    `Sex` UInt8,
    `Income` UInt8,
    `JavaEnable` UInt8,
    `CookieEnable` UInt8,
    `JavascriptEnable` UInt8,
    `IsMobile` UInt8,
    `BrowserLanguage` UInt16,
    `BrowserCountry` UInt16,
    `Interests` UInt16,
    `Robotness` UInt8,
    `GeneralInterests` Array(UInt16),
    `Params` Array(String),
    `Goals` Nested(
        ID UInt32,
        Serial UInt32,
        EventTime DateTime,
        Price Int64,
        OrderID String,
        CurrencyID UInt32),
    `WatchIDs` Array(UInt64),
    `ParamSumPrice` Int64,
    `ParamCurrency` FixedString(3),
    `ParamCurrencyID` UInt16,
    `ClickLogID` UInt64,
    `ClickEventID` Int32,
    `ClickGoodEvent` Int32,
    `ClickEventTime` DateTime,
    `ClickPriorityID` Int32,
    `ClickPhraseID` Int32,
    `ClickPageID` Int32,
    `ClickPlaceID` Int32,
    `ClickTypeID` Int32,
    `ClickResourceID` Int32,
    `ClickCost` UInt32,
    `ClickClientIP` UInt32,
    `ClickDomainID` UInt32,
    `ClickURL` String,
    `ClickAttempt` UInt8,
    `ClickOrderID` UInt32,
    `ClickBannerID` UInt32,
    `ClickMarketCategoryID` UInt32,
    `ClickMarketPP` UInt32,
    `ClickMarketCategoryName` String,
    `ClickMarketPPName` String,
    `ClickAWAPSCampaignName` String,
    `ClickPageName` String,
    `ClickTargetType` UInt16,
    `ClickTargetPhraseID` UInt64,
    `ClickContextType` UInt8,
    `ClickSelectType` Int8,
    `ClickOptions` String,
    `ClickGroupBannerID` Int32,
    `OpenstatServiceName` String,
    `OpenstatCampaignID` String,
    `OpenstatAdID` String,
    `OpenstatSourceID` String,
    `UTMSource` String,
    `UTMMedium` String,
    `UTMCampaign` String,
    `UTMContent` String,
    `UTMTerm` String,
    `FromTag` String,
    `HasGCLID` UInt8,
    `FirstVisit` DateTime,
    `PredLastVisit` Date,
    `LastVisit` Date,
    `TotalVisits` UInt32,
    `TraficSource` Nested(
        ID Int8,
        SearchEngineID UInt16,
        AdvEngineID UInt8,
        PlaceID UInt16,
        SocialSourceNetworkID UInt8,
        Domain String,
        SearchPhrase String,
        SocialSourcePage String),
    `Attendance` FixedString(16),
    `CLID` UInt32,
    `YCLID` UInt64,
    `NormalizedRefererHash` UInt64,
    `SearchPhraseHash` UInt64,
    `RefererDomainHash` UInt64,
    `NormalizedStartURLHash` UInt64,
    `StartURLDomainHash` UInt64,
    `NormalizedEndURLHash` UInt64,
    `TopLevelDomain` UInt64,
    `URLScheme` UInt64,
    `OpenstatServiceNameHash` UInt64,
    `OpenstatCampaignIDHash` UInt64,
    `OpenstatAdIDHash` UInt64,
    `OpenstatSourceIDHash` UInt64,
    `UTMSourceHash` UInt64,
    `UTMMediumHash` UInt64,
    `UTMCampaignHash` UInt64,
    `UTMContentHash` UInt64,
    `UTMTermHash` UInt64,
    `FromHash` UInt64,
    `WebVisorEnabled` UInt8,
    `WebVisorActivity` UInt32,
    `ParsedParams` Nested(
        Key1 String,
        Key2 String,
        Key3 String,
        Key4 String,
        Key5 String,
        ValueDouble Float64),
    `Market` Nested(
        Type UInt8,
        GoalID UInt32,
        OrderID String,
        OrderPrice Int64,
        PP UInt32,
        DirectPlaceID UInt32,
        DirectOrderID UInt32,
        DirectBannerID UInt32,
        GoodID String,
        GoodName String,
        GoodQuantity Int32,
        GoodPrice Int64),
    `IslandID` FixedString(16)
)
ENGINE = CollapsingMergeTree(Sign)
PARTITION BY toYYYYMM(StartDate)
ORDER BY (CounterID, StartDate, intHash32(UserID), VisitID)
SAMPLE BY intHash32(UserID)
SETTINGS index_granularity = 8192
```

Puede ejecutar esas consultas utilizando el modo interactivo de `clickhouse-client` (simplemente ejecútelo en un terminal sin especificar una consulta por adelantado) o pruebe algunos [interfaz alternativa](../interfaces/index.md) Si quieres.

Como podemos ver, `hits_v1` utiliza el [motor básico MergeTree](../engines/table-engines/mergetree-family/mergetree.md), mientras que el `visits_v1` utiliza el [Derrumbar](../engines/table-engines/mergetree-family/collapsingmergetree.md) variante.

### Importar datos {#import-data}

La importación de datos a ClickHouse se realiza a través de [INSERT INTO](../sql-reference/statements/insert-into.md) consulta como en muchas otras bases de datos SQL. Sin embargo, los datos generalmente se proporcionan en uno de los [Formatos de serialización compatibles](../interfaces/formats.md) en lugar de `VALUES` cláusula (que también es compatible).

Los archivos que descargamos anteriormente están en formato separado por tabuladores, así que aquí le mostramos cómo importarlos a través del cliente de la consola:

``` bash
clickhouse-client --query "INSERT INTO tutorial.hits_v1 FORMAT TSV" --max_insert_block_size=100000 < hits_v1.tsv
clickhouse-client --query "INSERT INTO tutorial.visits_v1 FORMAT TSV" --max_insert_block_size=100000 < visits_v1.tsv
```

ClickHouse tiene un montón de [ajustes para sintonizar](../operations/settings/index.md) y una forma de especificarlos en el cliente de la consola es a través de argumentos, como podemos ver con `--max_insert_block_size`. La forma más fácil de averiguar qué configuraciones están disponibles, qué significan y cuáles son los valores predeterminados es consultar el `system.settings` tabla:

``` sql
SELECT name, value, changed, description
FROM system.settings
WHERE name LIKE '%max_insert_b%'
FORMAT TSV

max_insert_block_size    1048576    0    "The maximum block size for insertion, if we control the creation of blocks for insertion."
```

Opcionalmente se puede [OPTIMIZE](../sql-reference/statements/misc.md#misc_operations-optimize) las tablas después de la importación. Las tablas que están configuradas con un motor de la familia MergeTree siempre fusionan partes de datos en segundo plano para optimizar el almacenamiento de datos (o al menos verificar si tiene sentido). Estas consultas obligan al motor de tablas a realizar la optimización del almacenamiento en este momento en lugar de algún tiempo después:

``` bash
clickhouse-client --query "OPTIMIZE TABLE tutorial.hits_v1 FINAL"
clickhouse-client --query "OPTIMIZE TABLE tutorial.visits_v1 FINAL"
```

Estas consultas inician una operación intensiva de E / S y CPU, por lo que si la tabla recibe datos nuevos de manera consistente, es mejor dejarlos solos y dejar que las fusiones se ejecuten en segundo plano.

Ahora podemos comprobar si la importación de la tabla fue exitosa:

``` bash
clickhouse-client --query "SELECT COUNT(*) FROM tutorial.hits_v1"
clickhouse-client --query "SELECT COUNT(*) FROM tutorial.visits_v1"
```

## Consultas de ejemplo {#example-queries}

``` sql
SELECT
    StartURL AS URL,
    AVG(Duration) AS AvgDuration
FROM tutorial.visits_v1
WHERE StartDate BETWEEN '2014-03-23' AND '2014-03-30'
GROUP BY URL
ORDER BY AvgDuration DESC
LIMIT 10
```

``` sql
SELECT
    sum(Sign) AS visits,
    sumIf(Sign, has(Goals.ID, 1105530)) AS goal_visits,
    (100. * goal_visits) / visits AS goal_percent
FROM tutorial.visits_v1
WHERE (CounterID = 912887) AND (toYYYYMM(StartDate) = 201403) AND (domain(StartURL) = 'yandex.ru')
```

## Implementación de clúster {#cluster-deployment}

El clúster ClickHouse es un clúster homogéneo. Pasos para configurar:

1.  Instale el servidor ClickHouse en todas las máquinas del clúster
2.  Configurar configuraciones de clúster en archivos de configuración
3.  Crear tablas locales en cada instancia
4.  Crear un [Tabla distribuida](../engines/table-engines/special/distributed.md)

[Tabla distribuida](../engines/table-engines/special/distributed.md) es en realidad una especie de “view” a las tablas locales del clúster ClickHouse. La consulta SELECT de una tabla distribuida se ejecuta utilizando recursos de todos los fragmentos del clúster. Puede especificar configuraciones para varios clústeres y crear varias tablas distribuidas que proporcionen vistas a diferentes clústeres.

Ejemplo de configuración para un clúster con tres fragmentos, una réplica cada uno:

``` xml
<remote_servers>
    <perftest_3shards_1replicas>
        <shard>
            <replica>
                <host>example-perftest01j.yandex.ru</host>
                <port>9000</port>
            </replica>
        </shard>
        <shard>
            <replica>
                <host>example-perftest02j.yandex.ru</host>
                <port>9000</port>
            </replica>
        </shard>
        <shard>
            <replica>
                <host>example-perftest03j.yandex.ru</host>
                <port>9000</port>
            </replica>
        </shard>
    </perftest_3shards_1replicas>
</remote_servers>
```

Para más demostraciones, vamos a crear una nueva tabla local con la misma `CREATE TABLE` consulta que utilizamos para `hits_v1`, pero nombre de tabla diferente:

``` sql
CREATE TABLE tutorial.hits_local (...) ENGINE = MergeTree() ...
```

Creación de una tabla distribuida que proporcione una vista en las tablas locales del clúster:

``` sql
CREATE TABLE tutorial.hits_all AS tutorial.hits_local
ENGINE = Distributed(perftest_3shards_1replicas, tutorial, hits_local, rand());
```

Una práctica común es crear tablas distribuidas similares en todas las máquinas del clúster. Permite ejecutar consultas distribuidas en cualquier máquina del clúster. También hay una opción alternativa para crear una tabla distribuida temporal para una consulta SELECT determinada usando [remoto](../sql-reference/table-functions/remote.md) función de la tabla.

Vamos a correr [INSERT SELECT](../sql-reference/statements/insert-into.md) en la tabla Distributed para extender la tabla a varios servidores.

``` sql
INSERT INTO tutorial.hits_all SELECT * FROM tutorial.hits_v1;
```

!!! warning "Aviso"
    Este enfoque no es adecuado para la fragmentación de tablas grandes. Hay una herramienta separada [Método de codificación de datos:](../operations/utilities/clickhouse-copier.md) que puede volver a fragmentar tablas grandes arbitrarias.

Como era de esperar, las consultas computacionalmente pesadas se ejecutan N veces más rápido si utilizan 3 servidores en lugar de uno.

En este caso, hemos utilizado un clúster con 3 fragmentos, y cada uno contiene una sola réplica.

Para proporcionar resiliencia en un entorno de producción, se recomienda que cada fragmento contenga 2-3 réplicas distribuidas entre varias zonas de disponibilidad o centros de datos (o al menos racks). Tenga en cuenta que ClickHouse admite un número ilimitado de réplicas.

Ejemplo de configuración para un clúster de un fragmento que contiene tres réplicas:

``` xml
<remote_servers>
    ...
    <perftest_1shards_3replicas>
        <shard>
            <replica>
                <host>example-perftest01j.yandex.ru</host>
                <port>9000</port>
             </replica>
             <replica>
                <host>example-perftest02j.yandex.ru</host>
                <port>9000</port>
             </replica>
             <replica>
                <host>example-perftest03j.yandex.ru</host>
                <port>9000</port>
             </replica>
        </shard>
    </perftest_1shards_3replicas>
</remote_servers>
```

Para habilitar la replicación nativa [ZooKeeper](http://zookeeper.apache.org/) se requiere. ClickHouse se encarga de la coherencia de los datos en todas las réplicas y ejecuta el procedimiento de restauración después de la falla automáticamente. Se recomienda implementar el clúster ZooKeeper en servidores independientes (donde no se están ejecutando otros procesos, incluido ClickHouse).

!!! note "Nota"
    ZooKeeper no es un requisito estricto: en algunos casos simples, puede duplicar los datos escribiéndolos en todas las réplicas de su código de aplicación. Este enfoque es **ni** recomendado, en este caso, ClickHouse no podrá garantizar la coherencia de los datos en todas las réplicas. Por lo tanto, se convierte en responsabilidad de su aplicación.

Las ubicaciones de ZooKeeper se especifican en el archivo de configuración:

``` xml
<zookeeper>
    <node>
        <host>zoo01.yandex.ru</host>
        <port>2181</port>
    </node>
    <node>
        <host>zoo02.yandex.ru</host>
        <port>2181</port>
    </node>
    <node>
        <host>zoo03.yandex.ru</host>
        <port>2181</port>
    </node>
</zookeeper>
```

Además, necesitamos establecer macros para identificar cada fragmento y réplica que se utilizan en la creación de tablas:

``` xml
<macros>
    <shard>01</shard>
    <replica>01</replica>
</macros>
```

Si no hay réplicas en este momento en la creación de la tabla replicada, se crea una instancia de una nueva primera réplica. Si ya hay réplicas activas, la nueva réplica clona los datos de las existentes. Tiene la opción de crear primero todas las tablas replicadas y, a continuación, insertar datos en ella. Otra opción es crear algunas réplicas y agregar las otras después o durante la inserción de datos.

``` sql
CREATE TABLE tutorial.hits_replica (...)
ENGINE = ReplcatedMergeTree(
    '/clickhouse_perftest/tables/{shard}/hits',
    '{replica}'
)
...
```

Aquí usamos [ReplicatedMergeTree](../engines/table-engines/mergetree-family/replication.md) motor de mesa. En los parámetros, especificamos la ruta ZooKeeper que contiene identificadores de fragmentos y réplicas.

``` sql
INSERT INTO tutorial.hits_replica SELECT * FROM tutorial.hits_local;
```

La replicación funciona en modo multi-master. Los datos se pueden cargar en cualquier réplica y el sistema los sincroniza automáticamente con otras instancias. La replicación es asíncrona, por lo que en un momento dado, no todas las réplicas pueden contener datos insertados recientemente. Al menos una réplica debe estar disponible para permitir la ingestión de datos. Otros sincronizarán los datos y repararán la coherencia una vez que vuelvan a activarse. Tenga en cuenta que este enfoque permite la baja posibilidad de una pérdida de datos recientemente insertados.

[Artículo Original](https://clickhouse.tech/docs/en/getting_started/tutorial/) <!--hide-->
