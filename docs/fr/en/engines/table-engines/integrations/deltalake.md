---
description: 'Ce moteur fournit un accès en lecture seule à des tables Delta Lake
  existantes dans Amazon S3.'
sidebar_label: 'DeltaLake'
sidebar_position: 40
slug: /engines/table-engines/integrations/deltalake
title: 'Moteur de table DeltaLake'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="deltalake-table-engine">
  # Moteur de table DeltaLake
</div>

Ce moteur fournit une intégration avec des tables [Delta Lake](https://github.com/delta-io/delta) existantes sur des stockages S3, GCP et Azure, et prend en charge à la fois la lecture et l’écriture (à partir de la v25.10).

<div id="create-table">
  ## Créer une table DeltaLake
</div>

Pour créer une table DeltaLake, celle-ci doit déjà exister dans un stockage S3, GCP ou Azure. Les commandes ci-dessous ne prennent pas de paramètres DDL pour créer une nouvelle table.

<Tabs>
  <TabItem value="S3" label="S3" default>
    **Syntaxe**

    ```sql
    CREATE TABLE table_name
    ENGINE = DeltaLake(url, [aws_access_key_id, aws_secret_access_key,] [extra_credentials])
    ```

    **Paramètres du moteur**

    * `url` — URL du bucket avec le chemin vers la table Delta Lake existante.
    * `aws_access_key_id`, `aws_secret_access_key` - Informations d’identification à long terme pour l’utilisateur du compte [AWS](https://aws.amazon.com/). Vous pouvez les utiliser pour authentifier vos requêtes. Ce paramètre est facultatif. Si ces informations d’identification ne sont pas spécifiées, celles du fichier de configuration sont utilisées.
    * `extra_credentials` - Facultatif. Utilisé pour transmettre un `role_arn` pour le contrôle d’accès basé sur les rôles dans ClickHouse Cloud. Voir [Secure S3](/fr/cloud/data-sources/secure-s3) pour les étapes de configuration.

    Les paramètres du moteur peuvent être spécifiés à l’aide de [collections nommées](/fr/operations/named-collections.md).

    **Exemple**

    ```sql
    CREATE TABLE deltalake
    ENGINE = DeltaLake('http://mars-doc-test.s3.amazonaws.com/clickhouse-bucket-3/test_table/', 'ABC123', 'Abc+123')
    ```

    Utilisation de collections nommées :

    ```xml
    <clickhouse>
        <named_collections>
            <deltalake_conf>
                <url>http://mars-doc-test.s3.amazonaws.com/clickhouse-bucket-3/</url>
                <access_key_id>ABC123</access_key_id>
                <secret_access_key>Abc+123</secret_access_key>
            </deltalake_conf>
        </named_collections>
    </clickhouse>
    ```

    ```sql
    CREATE TABLE deltalake
    ENGINE = DeltaLake(deltalake_conf, filename = 'test_table')
    ```
  </TabItem>

  <TabItem value="GCP" label="GCP" default>
    **Syntaxe**

    ```sql
    -- Utilisation d’une URL HTTPS (recommandée)
    CREATE TABLE table_name
    ENGINE = DeltaLake('https://storage.googleapis.com/<bucket>/<path>/', '<access_key_id>', '<secret_access_key>')
    ```

    :::note[URI gsutil non prise en charge]
    Les URI gsutil telles que `gs://clickhouse-docs-example-bucket` ne sont pas prises en charge ; veuillez utiliser une URL commençant par `https://storage.googleapis.com`
    :::

    **Arguments**

    * `url` — URL du bucket GCS vers la table Delta Lake. Doit utiliser le format `https://storage.googleapis.com/<bucket>/<path>/`
      (le point de terminaison de l’API XML GCS), ou `gs://<bucket>/<path>/`, qui est automatiquement converti.
    * `access_key_id` — Clé d’accès GCS. Créez-la via Google Cloud Console → Cloud Storage → Settings → Interoperability.
    * `secret_access_key` — Secret GCS.

    **Collections nommées**

    Vous pouvez également utiliser des collections nommées.
    Par exemple :

    ```sql
    CREATE NAMED COLLECTION gcs_creds AS
    access_key_id = '<access_key>',
    secret_access_key = '<secret>';

    CREATE TABLE gcpDeltaLake
    ENGINE = DeltaLake(gcs_creds, url = 'https://storage.googleapis.com/<bucket>/<path>')
    ```
  </TabItem>

  <TabItem value="Azure" label="Azure" default>
    **Syntaxe**

    ```sql
    CREATE TABLE table_name
    ENGINE = DeltaLake(connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression])
    ```

    **Arguments**

    * `connection_string` — chaîne de connexion Azure
    * `storage_account_url` — URL du compte de stockage Azure (par exemple : https://account.blob.core.windows.net)
    * `container_name` — nom du conteneur Azure
    * `blobpath` — chemin vers la table Delta Lake dans le conteneur
    * `account_name` — nom du compte de stockage Azure
    * `account_key` — clé du compte de stockage Azure
  </TabItem>
</Tabs>

<div id="insert-data">
  ## Écrire des données à l’aide d’une table DeltaLake
</div>

Une fois que vous avez créé une table avec le moteur de table DeltaLake, vous pouvez y insérer des données comme suit :

```sql
SET allow_delta_lake_writes = 1;

INSERT INTO deltalake(id, firstname, lastname, gender, age)
VALUES (1, 'John', 'Smith', 'M', 32);
```

:::note
L’écriture avec le moteur de table n’est prise en charge que via delta kernel.
Les écritures avec Azure ne sont pas encore prises en charge, mais elles le sont avec S3 et GCS.

L’écriture dans Delta Lake est une fonctionnalité Beta et doit être activée avec `SET allow_delta_lake_writes = 1` (disponible à partir de la version 26.7 ; sur les versions antérieures, utilisez `SET allow_experimental_delta_lake_writes = 1`).
:::

<div id="data-cache">
  ### Cache de données
</div>

Le moteur de table `DeltaLake` et la fonction de table prennent en charge la mise en cache des données, comme les stockages `S3`, `AzureBlobStorage` et `HDFS`. Voir [&quot;moteur de table S3&quot;](../../../engines/table-engines/integrations/s3.md#data-cache) pour plus de détails.

<div id="see-also">
  ## Voir aussi
</div>

* [fonction de table DeltaLake](../../../sql-reference/table-functions/deltalake.md)