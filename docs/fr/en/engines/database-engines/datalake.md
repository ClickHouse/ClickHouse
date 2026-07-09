---
description: 'Le moteur de base de données DataLakeCatalog vous permet de connecter ClickHouse à des catalogues de données externes et d’interroger des données dans des formats de table ouverts'
sidebar_label: 'DataLakeCatalog'
slug: /engines/database-engines/datalakecatalog
title: 'DataLakeCatalog'
doc_type: 'reference'
---

Le moteur de base de données `DataLakeCatalog` vous permet de connecter ClickHouse à des
catalogues de données externes et d’interroger des données dans des formats de table ouverts,
sans avoir à dupliquer les données.
Cela transforme ClickHouse en un puissant moteur de requêtes qui s’intègre parfaitement à
votre infrastructure de lac de données existante.

<div id="supported-catalogs">
  ## Catalogues pris en charge
</div>

Le moteur `DataLakeCatalog` prend en charge les catalogues de données suivants :

* **AWS Glue Catalog** - Pour les tables Iceberg dans les environnements AWS
* **Databricks Unity Catalog** - Pour les tables Delta Lake et Iceberg
* **Hive Metastore** - Catalogue traditionnel de l’écosystème Hadoop
* **REST Catalogs** - Tout catalogue compatible avec la spécification REST d’Iceberg

<div id="creating-a-database">
  ## Création d’une base de données
</div>

Vous devez activer les paramètres ci-dessous pour utiliser le moteur `DataLakeCatalog` :

```sql
SET allow_experimental_database_iceberg = 1;
SET allow_experimental_database_unity_catalog = 1;
SET allow_experimental_database_glue_catalog = 1;
SET allow_experimental_database_hms_catalog = 1;
SET allow_experimental_database_paimon_rest_catalog = 1;
```

Les bases de données utilisant le moteur `DataLakeCatalog` peuvent être créées selon la syntaxe suivante :

```sql
CREATE DATABASE database_name
ENGINE = DataLakeCatalog(catalog_endpoint[, user, password])
SETTINGS
catalog_type,
[...]
```

Les paramètres suivants sont pris en charge :

| Setting                 | Description                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| ----------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `catalog_type`          | Type de catalogue : `glue`, `unity` (Delta), `rest` (Iceberg), `hive`, `onelake` (Iceberg)                                                                                                                                                                                                                                                                                                                                                     |
| `warehouse`             | Nom de l’entrepôt/de la base de données à utiliser dans le catalogue.                                                                                                                                                                                                                                                                                                                                                                          |
| `catalog_credential`    | Informations d’authentification pour le catalogue (par ex. API key ou jeton)                                                                                                                                                                                                                                                                                                                                                                   |
| `auth_header`           | En-tête HTTP personnalisé pour l’authentification auprès du service de catalogue                                                                                                                                                                                                                                                                                                                                                               |
| `auth_scope`            | Scope OAuth2 pour l’authentification (si vous utilisez OAuth)                                                                                                                                                                                                                                                                                                                                                                                  |
| `storage_endpoint`      | URL du point de terminaison pour le stockage sous-jacent                                                                                                                                                                                                                                                                                                                                                                                       |
| `oauth_server_uri`      | URI du serveur d’autorisation OAuth2 pour l’authentification                                                                                                                                                                                                                                                                                                                                                                                   |
| `vended_credentials`    | Booléen indiquant s’il faut utiliser les informations d’authentification fournies par le catalogue (prend en charge AWS S3 et Azure ADLS Gen2)                                                                                                                                                                                                                                                                                                 |
| `aws_access_key_id`     | ID de clé d’accès AWS pour l’accès à S3/Glue (si vous n’utilisez pas les informations d’authentification fournies)                                                                                                                                                                                                                                                                                                                             |
| `aws_secret_access_key` | Clé d’accès secrète AWS pour l’accès à S3/Glue (si vous n’utilisez pas les informations d’authentification fournies)                                                                                                                                                                                                                                                                                                                           |
| `region`                | Région AWS du service (par ex. `us-east-1`)                                                                                                                                                                                                                                                                                                                                                                                                    |
| `dlf_access_key_id`     | ID de clé d’accès pour l’accès à DLF                                                                                                                                                                                                                                                                                                                                                                                                           |
| `dlf_access_key_secret` | Clé d’accès secrète pour l’accès à DLF                                                                                                                                                                                                                                                                                                                                                                                                         |
| `force_add_bucket`      | Lors de la construction des URL du stockage objet à partir de l’emplacement de la table fourni par le catalogue et de `storage_endpoint`, ajoutez en préfixe le nom du bucket/conteneur même si le point de terminaison le contient déjà. Valeur par défaut : `false`. Définissez `true` pour les catalogues qui renvoient des chemins sans le bucket et exigent qu’il soit ajouté lors de la construction de l’URL (chemins de type Polaris). |

<div id="examples">
  ## Exemples
</div>

Voir les sections ci-dessous pour obtenir des exemples d&#39;utilisation du moteur `DataLakeCatalog` :

* [Unity Catalog](/fr/use-cases/data-lake/unity-catalog)
* [Glue Catalog](/fr/use-cases/data-lake/glue-catalog)
* OneLake Catalog
  Peut être utilisé en activant `allow_experimental_database_iceberg` ou `allow_database_iceberg`.

```sql
CREATE DATABASE database_name
ENGINE = DataLakeCatalog(catalog_endpoint)
SETTINGS
    catalog_type = 'onelake',
    warehouse = warehouse,
    onelake_tenant_id = tenant_id,
    oauth_server_uri = server_uri,
    auth_scope = auth_scope,
    onelake_client_id = client_id,
    onelake_client_secret = client_secret;
SHOW TABLES IN database_name;
SELECT count() from database_name.table_name;
```