---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 41
toc_title: "Aplicaci\xF3n de modelos CatBoost"
---

# Aplicación de un modelo Catboost en ClickHouse {#applying-catboost-model-in-clickhouse}

[CatBoost](https://catboost.ai) es una biblioteca de impulso de gradiente libre y de código abierto desarrollada en [Yandex](https://yandex.com/company/) para el aprendizaje automático.

Con esta instrucción, aprenderá a aplicar modelos preentrenados en ClickHouse ejecutando la inferencia de modelos desde SQL.

Para aplicar un modelo CatBoost en ClickHouse:

1.  [Crear una tabla](#create-table).
2.  [Insertar los datos en la tabla](#insert-data-to-table).
3.  [Integrar CatBoost en ClickHouse](#integrate-catboost-into-clickhouse) (Paso opcional).
4.  [Ejecute la inferencia del modelo desde SQL](#run-model-inference).

Para obtener más información sobre la formación de modelos CatBoost, consulte [Entrenamiento y aplicación de modelos](https://catboost.ai/docs/features/training.html#training).

## Requisito {#prerequisites}

Si no tienes el [Acoplador](https://docs.docker.com/install/) sin embargo, instalarlo.

!!! note "Nota"
    [Acoplador](https://www.docker.com) es una plataforma de software que le permite crear contenedores que aíslan una instalación de CatBoost y ClickHouse del resto del sistema.

Antes de aplicar un modelo CatBoost:

**1.** Tire de la [Imagen de acoplador](https://hub.docker.com/r/yandex/tutorial-catboost-clickhouse) del registro:

``` bash
$ docker pull yandex/tutorial-catboost-clickhouse
```

Esta imagen de Docker contiene todo lo que necesita para ejecutar CatBoost y ClickHouse: código, tiempo de ejecución, bibliotecas, variables de entorno y archivos de configuración.

**2.** Asegúrese de que la imagen de Docker se haya extraído correctamente:

``` bash
$ docker image ls
REPOSITORY                            TAG                 IMAGE ID            CREATED             SIZE
yandex/tutorial-catboost-clickhouse   latest              622e4d17945b        22 hours ago        1.37GB
```

**3.** Inicie un contenedor Docker basado en esta imagen:

``` bash
$ docker run -it -p 8888:8888 yandex/tutorial-catboost-clickhouse
```

## 1. Crear una tabla {#create-table}

Para crear una tabla ClickHouse para el ejemplo de capacitación:

**1.** Inicie el cliente de consola ClickHouse en el modo interactivo:

``` bash
$ clickhouse client
```

!!! note "Nota"
    El servidor ClickHouse ya se está ejecutando dentro del contenedor Docker.

**2.** Cree la tabla usando el comando:

``` sql
:) CREATE TABLE amazon_train
(
    date Date MATERIALIZED today(),
    ACTION UInt8,
    RESOURCE UInt32,
    MGR_ID UInt32,
    ROLE_ROLLUP_1 UInt32,
    ROLE_ROLLUP_2 UInt32,
    ROLE_DEPTNAME UInt32,
    ROLE_TITLE UInt32,
    ROLE_FAMILY_DESC UInt32,
    ROLE_FAMILY UInt32,
    ROLE_CODE UInt32
)
ENGINE = MergeTree ORDER BY date
```

**3.** Salir del cliente de la consola ClickHouse:

``` sql
:) exit
```

## 2. Insertar los datos en la tabla {#insert-data-to-table}

Para insertar los datos:

**1.** Ejecute el siguiente comando:

``` bash
$ clickhouse client --host 127.0.0.1 --query 'INSERT INTO amazon_train FORMAT CSVWithNames' < ~/amazon/train.csv
```

**2.** Inicie el cliente de consola ClickHouse en el modo interactivo:

``` bash
$ clickhouse client
```

**3.** Asegúrese de que los datos se hayan cargado:

``` sql
:) SELECT count() FROM amazon_train

SELECT count()
FROM amazon_train

+-count()-+
|   65538 |
+-------+
```

## 3. Integrar CatBoost en ClickHouse {#integrate-catboost-into-clickhouse}

!!! note "Nota"
    **Paso opcional.** La imagen de Docker contiene todo lo que necesita para ejecutar CatBoost y ClickHouse.

Para integrar CatBoost en ClickHouse:

**1.** Construir la biblioteca de evaluación.

La forma más rápida de evaluar un modelo CatBoost es compilar `libcatboostmodel.<so|dll|dylib>` biblioteca. Para obtener más información acerca de cómo construir la biblioteca, vea [Documentación de CatBoost](https://catboost.ai/docs/concepts/c-plus-plus-api_dynamic-c-pluplus-wrapper.html).

**2.** Cree un nuevo directorio en cualquier lugar y con cualquier nombre, por ejemplo, `data` y poner la biblioteca creada en ella. La imagen de Docker ya contiene la biblioteca `data/libcatboostmodel.so`.

**3.** Cree un nuevo directorio para el modelo de configuración en cualquier lugar y con cualquier nombre, por ejemplo, `models`.

**4.** Cree un archivo de configuración de modelo con cualquier nombre, por ejemplo, `models/amazon_model.xml`.

**5.** Describir la configuración del modelo:

``` xml
<models>
    <model>
        <!-- Model type. Now catboost only. -->
        <type>catboost</type>
        <!-- Model name. -->
        <name>amazon</name>
        <!-- Path to trained model. -->
        <path>/home/catboost/tutorial/catboost_model.bin</path>
        <!-- Update interval. -->
        <lifetime>0</lifetime>
    </model>
</models>
```

**6.** Agregue la ruta de acceso a CatBoost y la configuración del modelo a la configuración de ClickHouse:

``` xml
<!-- File etc/clickhouse-server/config.d/models_config.xml. -->
<catboost_dynamic_library_path>/home/catboost/data/libcatboostmodel.so</catboost_dynamic_library_path>
<models_config>/home/catboost/models/*_model.xml</models_config>
```

## 4. Ejecute la inferencia del modelo desde SQL {#run-model-inference}

Para el modelo de prueba, ejecute el cliente ClickHouse `$ clickhouse client`.

Asegurémonos de que el modelo esté funcionando:

``` sql
:) SELECT
    modelEvaluate('amazon',
                RESOURCE,
                MGR_ID,
                ROLE_ROLLUP_1,
                ROLE_ROLLUP_2,
                ROLE_DEPTNAME,
                ROLE_TITLE,
                ROLE_FAMILY_DESC,
                ROLE_FAMILY,
                ROLE_CODE) > 0 AS prediction,
    ACTION AS target
FROM amazon_train
LIMIT 10
```

!!! note "Nota"
    Función [modelEvaluar](../sql-reference/functions/other-functions.md#function-modelevaluate) devuelve tupla con predicciones sin procesar por clase para modelos multiclase.

Vamos a predecir la probabilidad:

``` sql
:) SELECT
    modelEvaluate('amazon',
                RESOURCE,
                MGR_ID,
                ROLE_ROLLUP_1,
                ROLE_ROLLUP_2,
                ROLE_DEPTNAME,
                ROLE_TITLE,
                ROLE_FAMILY_DESC,
                ROLE_FAMILY,
                ROLE_CODE) AS prediction,
    1. / (1 + exp(-prediction)) AS probability,
    ACTION AS target
FROM amazon_train
LIMIT 10
```

!!! note "Nota"
    Más información sobre [exp()](../sql-reference/functions/math-functions.md) función.

Vamos a calcular LogLoss en la muestra:

``` sql
:) SELECT -avg(tg * log(prob) + (1 - tg) * log(1 - prob)) AS logloss
FROM
(
    SELECT
        modelEvaluate('amazon',
                    RESOURCE,
                    MGR_ID,
                    ROLE_ROLLUP_1,
                    ROLE_ROLLUP_2,
                    ROLE_DEPTNAME,
                    ROLE_TITLE,
                    ROLE_FAMILY_DESC,
                    ROLE_FAMILY,
                    ROLE_CODE) AS prediction,
        1. / (1. + exp(-prediction)) AS prob,
        ACTION AS tg
    FROM amazon_train
)
```

!!! note "Nota"
    Más información sobre [avg()](../sql-reference/aggregate-functions/reference.md#agg_function-avg) y [registro()](../sql-reference/functions/math-functions.md) función.

[Artículo Original](https://clickhouse.tech/docs/en/guides/apply_catboost_model/) <!--hide-->
