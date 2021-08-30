---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 37
toc_title: Fichier
---

# Fichier {#table_engines-file}

Le moteur de table de fichiers conserve les données dans un fichier dans l'un des [fichier
format](../../../interfaces/formats.md#formats) (TabSeparated, Native, etc.).

Exemples d'utilisation:

-   Exportation de données de ClickHouse vers un fichier.
-   Convertir des données d'un format à un autre.
-   Mise à jour des données dans ClickHouse via l'édition d'un fichier sur un disque.

## Utilisation dans le serveur ClickHouse {#usage-in-clickhouse-server}

``` sql
File(Format)
```

Le `Format` paramètre spécifie l'un des formats de fichier disponibles. Effectuer
`SELECT` requêtes, le format doit être pris en charge pour l'entrée, et à effectuer
`INSERT` queries – for output. The available formats are listed in the
[Format](../../../interfaces/formats.md#formats) section.

ClickHouse ne permet pas de spécifier le chemin du système de fichiers pour`File`. Il utilisera le dossier défini par [chemin](../../../operations/server-configuration-parameters/settings.md) réglage dans la configuration du serveur.

Lors de la création de la table en utilisant `File(Format)` il crée sous-répertoire vide dans ce dossier. Lorsque les données sont écrites dans cette table, elles sont mises dans `data.Format` fichier dans ce répertoire.

Vous pouvez créer manuellement ce sous dossier et ce fichier dans le système de fichiers [ATTACH](../../../sql-reference/statements/misc.md) il à la table des informations avec le nom correspondant, de sorte que vous pouvez interroger les données de ce fichier.

!!! warning "Avertissement"
    Soyez prudent avec cette fonctionnalité, car ClickHouse ne garde pas trace des modifications externes apportées à ces fichiers. Le résultat des Écritures simultanées via ClickHouse et en dehors de ClickHouse n'est pas défini.

**Exemple:**

**1.** Configurer le `file_engine_table` table:

``` sql
CREATE TABLE file_engine_table (name String, value UInt32) ENGINE=File(TabSeparated)
```

Par défaut ClickHouse va créer un dossier `/var/lib/clickhouse/data/default/file_engine_table`.

**2.** Créer manuellement `/var/lib/clickhouse/data/default/file_engine_table/data.TabSeparated` contenant:

``` bash
$ cat data.TabSeparated
one 1
two 2
```

**3.** Interroger les données:

``` sql
SELECT * FROM file_engine_table
```

``` text
┌─name─┬─value─┐
│ one  │     1 │
│ two  │     2 │
└──────┴───────┘
```

## Utilisation dans ClickHouse-local {#usage-in-clickhouse-local}

Dans [clickhouse-local](../../../operations/utilities/clickhouse-local.md) Fichier moteur accepte chemin d'accès au fichier en plus `Format`. Les flux d'entrée / sortie par défaut peuvent être spécifiés en utilisant des noms numériques ou lisibles par l'homme comme `0` ou `stdin`, `1` ou `stdout`.
**Exemple:**

``` bash
$ echo -e "1,2\n3,4" | clickhouse-local -q "CREATE TABLE table (a Int64, b Int64) ENGINE = File(CSV, stdin); SELECT a, b FROM table; DROP TABLE table"
```

## Les détails de mise en Œuvre {#details-of-implementation}

-   Plusieurs `SELECT` les requêtes peuvent être effectuées simultanément, mais `INSERT` requêtes s'attendre les uns les autres.
-   Prise en charge de la création d'un nouveau fichier par `INSERT` requête.
-   Si le fichier existe, `INSERT` ajouterait de nouvelles valeurs dedans.
-   Pas pris en charge:
    -   `ALTER`
    -   `SELECT ... SAMPLE`
    -   Index
    -   Réplication

[Article Original](https://clickhouse.tech/docs/en/operations/table_engines/file/) <!--hide-->
