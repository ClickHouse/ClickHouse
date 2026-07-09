---
description: 'Crée une table à partir de l’`URL` avec le `format` et la `structure` spécifiés'
sidebar_label: 'url'
sidebar_position: 200
slug: /sql-reference/table-functions/url
title: 'url'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="url-table-function">
  # Fonction de table `url`
</div>

La fonction `url` crée une table à partir de l’`URL` spécifiée, avec le `format` et la `structure` indiqués.

La fonction `url` peut être utilisée dans les requêtes `SELECT` et `INSERT` sur les données de tables [URL](../../engines/table-engines/special/url.md).

<div id="syntax">
  ## Syntaxe
</div>

```sql
url(URL [,format] [,structure] [,headers])
```

<div id="parameters">
  ## Paramètres
</div>

| Paramètre   | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `URL`       | Une URL entre apostrophes dont le schéma sélectionne le backend. Une URL `http`/`https` (ou non reconnue) est une adresse de serveur qui accepte les requêtes `GET` ou `POST` (pour les requêtes `SELECT` ou `INSERT`, respectivement) ; un schéma non HTTP reconnu (`file://`, `s3://`, `az://`, `hdfs://`, …) est redirigé vers la fonction de table correspondante — voir [routage selon le schéma d’URL](#scheme-dispatch). Type : [String](../../sql-reference/data-types/string.md). |
| `format`    | [Format](/fr/sql-reference/formats) des données. Type : [String](../../sql-reference/data-types/string.md).                                                                                                                                                                                                                                                                                                                                                                                   |
| `structure` | Structure de la table au format `'UserID UInt64, Name String'`. Détermine les noms et les types des colonnes. Type : [String](../../sql-reference/data-types/string.md).                                                                                                                                                                                                                                                                                                                   |
| `headers`   | En-têtes au format `'headers('key1'='value1', 'key2'='value2')'`. Vous pouvez définir des en-têtes pour l’appel HTTP.                                                                                                                                                                                                                                                                                                                                                                      |

<div id="returned_value">
  ## Valeur renvoyée
</div>

Une table au format et à la structure spécifiés, contenant les données de l’`URL` définie.

<div id="examples">
  ## Exemples
</div>

Récupération des 3 premières lignes d’une table contenant des colonnes de type `String` et [UInt32](../../sql-reference/data-types/int-uint.md) depuis un serveur HTTP qui répond au format [CSV](/fr/interfaces/formats/CSV).

```sql
SELECT * FROM url('http://127.0.0.1:12345/', CSV, 'column1 String, column2 UInt32', headers('Accept'='text/csv; charset=utf-8')) LIMIT 3;
```

Insertion de données depuis une `URL` dans une table :

```sql
CREATE TABLE test_table (column1 String, column2 UInt32) ENGINE=Memory;
INSERT INTO FUNCTION url('http://127.0.0.1:8123/?query=INSERT+INTO+test_table+FORMAT+CSV', 'CSV', 'column1 String, column2 UInt32') VALUES ('http interface', 42);
SELECT * FROM test_table;
```

<div id="scheme-dispatch">
  ## Routage selon le schéma d’URL
</div>

La fonction `url` sert de wrapper unifié au-dessus des autres fonctions de table pour les fichiers et le stockage d’objets : elle route vers le bon backend selon le schéma d’URL. Cela vous permet de lire depuis n’importe quel emplacement pris en charge avec une syntaxe unique et uniforme.

| Schéma                                       | Routé vers                                            |
| -------------------------------------------- | ----------------------------------------------------- |
| `http`, `https` (et tout schéma non reconnu) | le moteur `URL` lui-même (HTTP `GET`/`POST`)          |
| `file`                                       | la fonction [`file`](file.md)                         |
| `s3`, `gs`, `gcs`, `oss`                     | la fonction [`s3`](s3.md)                             |
| `az`, `azure`, `abfss`, `abfs`               | la fonction [`azureBlobStorage`](azureBlobStorage.md) |
| `hdfs`                                       | la fonction [`hdfs`](hdfs.md)                         |

Seuls les schémas S3 que le mappeur d’URI S3 résout vers un endpoint concret sans configuration supplémentaire (`s3`, ainsi que `gs`/`gcs`/`oss`) sont routés. Les autres schémas de fournisseurs S3-compatible (`cos`, `obs`, `eos`, …) sont spécifiques à une région et n’ont pas de mapping d’endpoint par défaut. Une URL `cos://…` est donc traitée comme un schéma non reconnu et signalée comme une erreur ; pour ces backends, utilisez directement la fonction [`s3`](s3.md) (avec `url_scheme_mappers` configuré).

Pour `file://`, un path relatif (`file://data.csv`) est résolu dans le directory [user&#95;files](/fr/operations/server-configuration-parameters/settings#user_files_path), et un path absolu (`file:///home/user/data.csv`) doit, comme d’habitude, pointer à l’intérieur de celui-ci.

Les arguments `format`, `structure` et `compression_method`, ainsi que le réglage [url&#95;base](#resolving-relative-urls), fonctionnent de la même manière quelle que soit la cible du routage.

```sql
SELECT * FROM url('file://data.csv', CSV, 'a UInt32, b String');
SELECT * FROM url('s3://clickhouse-public-datasets/hits_compatible/hits.csv');
```

Le routage selon le schéma n’est pas encore pris en charge par [`urlCluster`](urlCluster.md) : tout schéma autre que `http(s)` passé à `urlCluster` est rejeté avec une erreur. Utilisez plutôt la fonction cluster correspondante (`s3Cluster`, `azureBlobStorageCluster`, `hdfsCluster`, …) pour ces backends.

<div id="globs-in-url">
  ## Globs dans l’URL
</div>

Les motifs entre `{ }` sont utilisés pour générer un ensemble de shards ou pour spécifier des adresses de failover. Pour les types de motifs pris en charge et des exemples, consultez la description de la fonction [remote](remote.md#globs-in-addresses).
Le caractère `|` à l’intérieur des motifs est utilisé pour spécifier des adresses de failover. Celles-ci sont parcourues dans le même ordre que celui indiqué dans le motif. Le nombre d’adresses générées est limité par le paramètre [glob&#95;expansion&#95;max&#95;elements](../../operations/settings/settings.md#glob_expansion_max_elements).
Pour la syntaxe des globs de chemin dans le chemin de l’URL (par exemple `*`, `{a,b}`, `{N..M}` et `**`), voir [Globs dans le chemin](file.md#globs-in-path). Notez que `?` marque le début de la chaîne de requête dans une URL et ne peut pas être utilisé comme caractère générique dans le composant de chemin.

<div id="wildcards-with-http-index-pages">
  ## Caractères génériques avec les pages d’index HTTP
</div>

Pour `url` et le moteur de table `URL`, ClickHouse peut développer les caractères génériques en récupérant des pages d’index HTTP (HTML ou texte brut) et en extrayant les URL du corps de la réponse. Cela permet des motifs comme `/**/` lorsque le serveur expose des listings de répertoires.

Remarques :

* Les URL relatives sont résolues par rapport à l’URL de la page d’index.
* Les modèles `URL` sont développés avant la récupération des pages d’index, y compris l’expansion des shards par virgules et plages numériques, ainsi que les options de failover `|` en dehors du composant de chemin.
* Les motifs de failover `|` à l’intérieur du composant de chemin ne sont pas pris en charge pour l’expansion des pages d’index HTTP.
* La correspondance des caractères génériques s’applique au composant de chemin de l’URL.
* Si une URL listée contient déjà une chaîne de requête ou un fragment, celle-ci a préséance sur ceux de l’URL source. Sinon, la chaîne de requête et le fragment de l’URL source sont utilisés.
* Une liste vide est autorisée ; les erreurs HTTP (par ex. 404) sur les pages d’index provoquent des exceptions.
* La taille maximale de la page d’index est limitée par [max&#95;http&#95;index&#95;page&#95;size](/fr/operations/server-configuration-parameters/settings.md#max_http_index_page_size).
* Le nombre maximal de répertoires lus pendant l’expansion récursive est limité par [url&#95;wildcard&#95;max&#95;directories&#95;to&#95;read](/fr/operations/settings/settings.md#url_wildcard_max_directories_to_read).

Exemple :

```sql
SELECT count()
FROM url('https://ftp.gnu.org/gnu/wget/wget-1.21*.tar.gz', 'RawBLOB')
SETTINGS max_threads = 1, allow_experimental_url_wildcard_from_index_pages = 1;
```

<div id="virtual-columns">
  ## Colonnes virtuelles
</div>

* `_path` — Chemin de l&#39;`URL`. Type : `LowCardinality(String)`.
* `_file` — Nom de la ressource dans l&#39;`URL`. Type : `LowCardinality(String)`.
* `_size` — Taille de la ressource en octets. Type : `Nullable(UInt64)`. Si la taille est inconnue, la valeur est `NULL`.
* `_time` — Date et heure de dernière modification du fichier. Type : `Nullable(DateTime)`. Si cette date/heure est inconnue, la valeur est `NULL`.
* `_headers` - En-têtes de la réponse HTTP. Type : `Map(LowCardinality(String), LowCardinality(String))`.

<div id="hive-style-partitioning">
  ## paramètre use_hive_partitioning
</div>

Lorsque le paramètre `use_hive_partitioning` est défini sur 1, ClickHouse détecte le partitionnement de type Hive dans le chemin (`/name=value/`) et permet d’utiliser les colonnes de partition comme colonnes virtuelles dans la requête. Ces colonnes virtuelles auront les mêmes noms que dans le chemin partitionné.

**Exemple**

Utiliser une colonne virtuelle créée avec le partitionnement de type Hive

```sql
SELECT * FROM url('http://data/path/date=*/country=*/code=*/*.parquet') WHERE date > '2020-01-01' AND country = 'Netherlands' AND code = 42;
```

<div id="resolving-relative-urls">
  ## Résolution des URL relatives
</div>

Le paramètre [url&#95;base](/fr/operations/settings/settings.md#url_base) permet de transmettre une URL relative à la fonction `url`. Lorsque `url_base` est défini et que l’argument de la fonction est une référence relative, celle-ci est résolue par rapport à l’URL de base conformément à la [RFC 3986](https://datatracker.ietf.org/doc/html/rfc3986).

Les règles de résolution sont les suivantes :

* **Relative au chemin** (par ex. `data.csv`) : combinée avec le chemin de l’URL de base — tout ce qui suit le dernier `/` du chemin de base est remplacé. La barre oblique finale est importante : `https://example.com/dir/` + `data.csv` donne `https://example.com/dir/data.csv`, mais `https://example.com/dir` + `data.csv` donne `https://example.com/data.csv`. Les segments de point (`./` et `../`) sont normalisés.
* **Relative à l’hôte** (par ex. `/test/data.csv`) : résolue à l’aide du schéma et de l’hôte de l’URL de base.
* **Relative au schéma** (par ex. `//other.com/test/data.csv`) : résolue à l’aide du schéma de l’URL de base.
* **Requête uniquement** (par ex. `?x=1`) : ajoutée au chemin de base complet, en remplaçant toute requête ou tout fragment existant.
* **Fragment uniquement** (par ex. `#frag`) : ajouté à l’URL de base, en conservant la requête et en remplaçant tout fragment existant.
* **Vide** : renvoie l’URL de base sans fragment.
* **URL absolue** : transmise telle quelle ; `url_base` est ignoré.

**Exemple**

```sql
SET url_base = 'https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/';
SELECT * FROM url('tests/queries/0_stateless/data_csv/data.csv', CSV) LIMIT 3;
```

<div id="storage-settings">
  ## Paramètres de stockage
</div>

* [engine&#95;url&#95;skip&#95;empty&#95;files](/fr/operations/settings/settings.md#engine_url_skip_empty_files) - permet d’ignorer les fichiers vides lors de la lecture. Désactivé par défaut.
* [enable&#95;url&#95;encoding](/fr/operations/settings/settings.md#enable_url_encoding) - permet d’activer/de désactiver le décodage/l’encodage du chemin dans l’URI. Activé par défaut.
* [url&#95;base](/fr/operations/settings/settings.md#url_base) - URL de base pour la résolution des URL relatives passées à la fonction `url`.

<div id="permissions">
  ## Autorisations
</div>

La fonction `url` nécessite l’autorisation `CREATE TEMPORARY TABLE`. Par conséquent, elle ne fonctionnera pas pour les utilisateurs ayant le paramètre [readonly](/fr/operations/settings/permissions-for-queries#readonly) = 1. Au minimum, readonly = 2 est requis.

<div id="related">
  ## Voir aussi
</div>

* [Colonnes virtuelles](/fr/engines/table-engines/index.md#table_engines-virtual_columns)