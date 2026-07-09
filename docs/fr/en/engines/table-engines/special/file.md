---
description: 'Le moteur de table File conserve les données dans un fichier dans l’un
  des formats de fichier pris en charge (`TabSeparated`, `Native`, etc.).'
sidebar_label: 'File'
sidebar_position: 40
slug: /engines/table-engines/special/file
title: 'Moteur de table File'
doc_type: 'reference'
---

Le moteur de table File conserve les données dans un fichier dans l’un des [formats de fichier](/fr/interfaces/formats#formats-overview) pris en charge (`TabSeparated`, `Native`, etc.).

Cas d’utilisation :

* Exporter des données de ClickHouse vers un fichier.
* Convertir des données d’un format à un autre.
* Mettre à jour des données dans ClickHouse en modifiant un fichier sur le disque.

:::note
Ce moteur n’est actuellement pas disponible dans ClickHouse Cloud. Veuillez [utiliser plutôt la fonction de table S3](/fr/sql-reference/table-functions/s3.md).
:::

<div id="usage-in-clickhouse-server">
  ## Utilisation dans ClickHouse Server
</div>

```sql
File(Format)
```

Le paramètre `Format` spécifie l’un des formats de fichier disponibles. Pour effectuer des
requêtes `SELECT`, le format doit être pris en charge en entrée, et pour effectuer des
requêtes `INSERT`, en sortie. Les formats disponibles sont répertoriés dans la
section [Formats](/fr/interfaces/formats#formats-overview).

ClickHouse ne permet pas de spécifier le chemin du système de fichiers pour `File`. Il utilise le dossier défini par le paramètre [path](../../../operations/server-configuration-parameters/settings.md) dans la configuration du serveur.

Lors de la création d’une table avec `File(Format)`, un sous-répertoire vide est créé dans ce dossier. Lorsque des données sont écrites dans cette table, elles sont placées dans le fichier `data.Format` de ce sous-répertoire.

Vous pouvez créer manuellement ce sous-dossier et ce fichier dans le système de fichiers du serveur, puis l’[ATTACH](../../../sql-reference/statements/attach.md) aux métadonnées d’une table portant le même nom, afin de pouvoir interroger les données de ce fichier.

:::note
Soyez prudent avec cette fonctionnalité, car ClickHouse ne suit pas les modifications externes apportées à ces fichiers. Le résultat d’écritures simultanées effectuées via ClickHouse et en dehors de ClickHouse est indéfini.
:::

<div id="example">
  ## Exemple
</div>

**1.** Configurez la table `file_engine_table` :

```sql
CREATE TABLE file_engine_table (name String, value UInt32) ENGINE=File(TabSeparated)
```

Par défaut, ClickHouse crée le dossier `/var/lib/clickhouse/data/default/file_engine_table`.

**2.** Créez manuellement le fichier `/var/lib/clickhouse/data/default/file_engine_table/data.TabSeparated` contenant :

```bash
$ cat data.TabSeparated
one 1
two 2
```

**3.** Interrogez les données :

```sql
SELECT * FROM file_engine_table
```

```text
┌─name─┬─value─┐
│ one  │     1 │
│ two  │     2 │
└──────┴───────┘
```

<div id="usage-in-clickhouse-local">
  ## Utilisation dans ClickHouse-local
</div>

Dans [clickhouse-local](../../../operations/utilities/clickhouse-local.md), le moteur File accepte un chemin de fichier en plus de `Format`. Les flux d’entrée/sortie par défaut peuvent être spécifiés à l’aide de noms numériques ou lisibles par un humain, comme `0` ou `stdin`, `1` ou `stdout`. Il est possible de lire et d’écrire des fichiers compressés selon un paramètre supplémentaire du moteur ou l’extension du fichier (`gz`, `br` ou `xz`).

**Exemple :**

```bash
$ echo -e "1,2\n3,4" | clickhouse-local -q "CREATE TABLE table (a Int64, b Int64) ENGINE = File(CSV, stdin); SELECT a, b FROM table; DROP TABLE table"
```

<div id="details-of-implementation">
  ## Détails de l’implémentation
</div>

* Plusieurs requêtes `SELECT` peuvent être exécutées simultanément, mais les requêtes `INSERT` doivent attendre les unes après les autres.
* La création d’un nouveau fichier via une requête `INSERT` est prise en charge.
* Si le fichier existe, `INSERT` y ajoutera de nouvelles valeurs.
* Non pris en charge :
  * `ALTER`
  * `SELECT ... SAMPLE`
  * Indices
  * Réplication

<div id="partition-by">
  ## PARTITION BY
</div>

`PARTITION BY` — Facultatif. Il est possible de créer des fichiers distincts en partitionnant les données selon une clé de partition. Dans la plupart des cas, vous n’avez pas besoin de clé de partition et, si elle est nécessaire, elle n’a généralement pas besoin d’être plus granulaire qu’un partitionnement mensuel. Le partitionnement n’accélère pas les requêtes (contrairement à l’expression ORDER BY). Vous ne devez jamais utiliser un partitionnement trop fin. Ne partitionnez pas vos données par identifiant ou nom de client (utilisez plutôt l’identifiant ou le nom du client comme première colonne de l’expression ORDER BY).

Pour un partitionnement par mois, utilisez l’expression `toYYYYMM(date_column)`, où `date_column` est une colonne contenant une date de type [Date](/fr/sql-reference/data-types/date.md). Les noms de partition ont ici le format `"YYYYMM"`.

<div id="virtual-columns">
  ## Colonnes virtuelles
</div>

* `_path` — Chemin du fichier. Type : `LowCardinality(String)`.
* `_file` — Nom du fichier. Type : `LowCardinality(String)`.
* `_size` — Taille du fichier en octets. Type : `Nullable(UInt64)`. Si la taille est inconnue, la valeur est `NULL`.
* `_time` — Date et heure de la dernière modification du fichier. Type : `Nullable(DateTime)`. Si la date et l&#39;heure sont inconnues, la valeur est `NULL`.

<div id="settings">
  ## Paramètres
</div>

* [engine&#95;file&#95;empty&#95;if&#95;not&#95;exists](/fr/operations/settings/settings#engine_file_empty_if_not_exists) - permet de renvoyer des données vides à partir d’un fichier inexistant. Désactivé par défaut.
* [engine&#95;file&#95;truncate&#95;on&#95;insert](/fr/operations/settings/settings#engine_file_truncate_on_insert) - permet de tronquer le fichier avant d’y insérer des données. Désactivé par défaut.
* [engine&#95;file&#95;allow&#95;create&#95;multiple&#95;files](/fr/operations/settings/settings.md#engine_file_allow_create_multiple_files) - permet de créer un nouveau fichier à chaque insertion si le format comporte un suffixe. Désactivé par défaut.
* [engine&#95;file&#95;skip&#95;empty&#95;files](/fr/operations/settings/settings.md#engine_file_skip_empty_files) - permet d’ignorer les fichiers vides lors de la lecture. Désactivé par défaut.
* [storage&#95;file&#95;read&#95;method](/fr/operations/settings/settings#engine_file_empty_if_not_exists) - méthode de lecture des données depuis le fichier de stockage, parmi : `read`, `pread`, `mmap`. La méthode `mmap` ne s’applique pas à clickhouse-server (elle est destinée à clickhouse-local). Valeur par défaut : `pread` pour clickhouse-server, `mmap` pour clickhouse-local.