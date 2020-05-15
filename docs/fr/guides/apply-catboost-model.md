---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 41
toc_title: "Application Des Mod\xE8les CatBoost"
---

# Application D'un modèle Catboost dans ClickHouse {#applying-catboost-model-in-clickhouse}

[CatBoost](https://catboost.ai) est une bibliothèque de dynamisation de gradient libre et open-source développée à [Yandex](https://yandex.com/company/) pour l'apprentissage automatique.

Avec cette instruction, vous apprendrez à appliquer des modèles pré-formés dans ClickHouse en exécutant l'inférence de modèle à partir de SQL.

Pour appliquer un modèle CatBoost dans ClickHouse:

1.  [Créer une Table](#create-table).
2.  [Insérez les données dans la Table](#insert-data-to-table).
3.  [Intégrer CatBoost dans ClickHouse](#integrate-catboost-into-clickhouse) (Étape facultative).
4.  [Exécutez L'inférence du modèle à partir de SQL](#run-model-inference).

Pour plus d'informations sur la formation des modèles CatBoost, voir [Formation et application de modèles](https://catboost.ai/docs/features/training.html#training).

## Préalable {#prerequisites}

Si vous n'avez pas le [Docker](https://docs.docker.com/install/) pourtant, l'installer.

!!! note "Note"
    [Docker](https://www.docker.com) est une plate-forme logicielle qui vous permet de créer des conteneurs qui isolent une installation CatBoost et ClickHouse du reste du système.

Avant d'appliquer un modèle CatBoost:

**1.** Tirez la [Docker image](https://hub.docker.com/r/yandex/tutorial-catboost-clickhouse) à partir du registre:

``` bash
$ docker pull yandex/tutorial-catboost-clickhouse
```

Cette image Docker contient tout ce dont vous avez besoin pour exécuter CatBoost et ClickHouse: code, runtime, bibliothèques, variables d'environnement et fichiers de configuration.

**2.** Assurez-vous que l'image Docker a été tirée avec succès:

``` bash
$ docker image ls
REPOSITORY                            TAG                 IMAGE ID            CREATED             SIZE
yandex/tutorial-catboost-clickhouse   latest              622e4d17945b        22 hours ago        1.37GB
```

**3.** Démarrer un conteneur Docker basé sur cette image:

``` bash
$ docker run -it -p 8888:8888 yandex/tutorial-catboost-clickhouse
```

## 1. Créer une Table {#create-table}

Pour créer une table ClickHouse pour l'exemple de formation:

**1.** Démarrez clickhouse console client en mode interactif:

``` bash
$ clickhouse client
```

!!! note "Note"
    Le serveur ClickHouse est déjà en cours d'exécution dans le conteneur Docker.

**2.** Créer la table à l'aide de la commande:

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

**3.** Quitter le client de la console ClickHouse:

``` sql
:) exit
```

## 2. Insérez les données dans la Table {#insert-data-to-table}

Pour insérer les données:

**1.** Exécutez la commande suivante:

``` bash
$ clickhouse client --host 127.0.0.1 --query 'INSERT INTO amazon_train FORMAT CSVWithNames' < ~/amazon/train.csv
```

**2.** Démarrez clickhouse console client en mode interactif:

``` bash
$ clickhouse client
```

**3.** Assurez-vous que les données ont été téléchargées:

``` sql
:) SELECT count() FROM amazon_train

SELECT count()
FROM amazon_train

+-count()-+
|   65538 |
+-------+
```

## 3. Intégrer CatBoost dans ClickHouse {#integrate-catboost-into-clickhouse}

!!! note "Note"
    **Étape facultative.** L'image Docker contient tout ce dont vous avez besoin pour exécuter CatBoost et ClickHouse.

Pour intégrer CatBoost dans ClickHouse:

**1.** Construire la bibliothèque d'évaluation.

Le moyen le plus rapide d'évaluer un modèle CatBoost est la compilation `libcatboostmodel.<so|dll|dylib>` bibliothèque. Pour plus d'informations sur la création de la bibliothèque, voir [Documentation CatBoost](https://catboost.ai/docs/concepts/c-plus-plus-api_dynamic-c-pluplus-wrapper.html).

**2.** Créez un nouveau répertoire n'importe où et avec n'importe quel nom, par exemple, `data` et mettez la bibliothèque créée dedans. L'image Docker contient déjà la bibliothèque `data/libcatboostmodel.so`.

**3.** Créez un nouveau répertoire pour le modèle de configuration n'importe où et avec n'importe quel nom, par exemple, `models`.

**4.** Créez un fichier de configuration de modèle avec n'importe quel nom, par exemple, `models/amazon_model.xml`.

**5.** Décrire la configuration du modèle:

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

**6.** Ajoutez le chemin D'accès à CatBoost et la configuration du modèle à la configuration de ClickHouse:

``` xml
<!-- File etc/clickhouse-server/config.d/models_config.xml. -->
<catboost_dynamic_library_path>/home/catboost/data/libcatboostmodel.so</catboost_dynamic_library_path>
<models_config>/home/catboost/models/*_model.xml</models_config>
```

## 4. Exécutez L'inférence du modèle à partir de SQL {#run-model-inference}

Pour le modèle de test exécutez le client ClickHouse `$ clickhouse client`.

Assurons nous que le modèle fonctionne:

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

!!! note "Note"
    Fonction [modelEvaluate](../sql-reference/functions/other-functions.md#function-modelevaluate) retourne tuple avec des prédictions brutes par classe pour les modèles multiclasse.

Prédisons la probabilité:

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

!!! note "Note"
    Plus d'infos sur [exp()](../sql-reference/functions/math-functions.md) fonction.

Calculons LogLoss sur l'échantillon:

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

!!! note "Note"
    Plus d'infos sur [avg()](../sql-reference/aggregate-functions/reference.md#agg_function-avg) et [journal()](../sql-reference/functions/math-functions.md) fonction.

[Article Original](https://clickhouse.tech/docs/en/guides/apply_catboost_model/) <!--hide-->
