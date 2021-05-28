---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 39
toc_title: GRANT
---

# GRANT {#grant}

-   Accorder [privilège](#grant-privileges) pour ClickHouse comptes d'utilisateurs ou des rôles.
-   Affecte des rôles à des comptes d'utilisateurs ou à d'autres rôles.

Pour révoquer les privilèges, utilisez [REVOKE](revoke.md) déclaration. Vous pouvez également classer les privilèges accordés par le [SHOW GRANTS](show.md#show-grants-statement) déclaration.

## Accorder La Syntaxe Des Privilèges {#grant-privigele-syntax}

``` sql
GRANT [ON CLUSTER cluster_name] privilege[(column_name [,...])] [,...] ON {db.table|db.*|*.*|table|*} TO {user | role | CURRENT_USER} [,...] [WITH GRANT OPTION]
```

-   `privilege` — Type of privilege.
-   `role` — ClickHouse user role.
-   `user` — ClickHouse user account.

Le `WITH GRANT OPTION` clause de subventions `user` ou `role` avec l'autorisation de réaliser des `GRANT` requête. Les utilisateurs peuvent accorder des privilèges de la même portée qu'ils ont et moins.

## Attribution De La Syntaxe Du Rôle {#assign-role-syntax}

``` sql
GRANT [ON CLUSTER cluster_name] role [,...] TO {user | another_role | CURRENT_USER} [,...] [WITH ADMIN OPTION]
```

-   `role` — ClickHouse user role.
-   `user` — ClickHouse user account.

Le `WITH ADMIN OPTION` clause de jeux [ADMIN OPTION](#admin-option-privilege) privilège pour `user` ou `role`.

## Utilisation {#grant-usage}

Utiliser `GRANT` votre compte doit avoir le `GRANT OPTION` privilège. Vous ne pouvez accorder des privilèges que dans le cadre de vos privilèges de Compte.

Par exemple, l'administrateur a accordé des privilèges `john` compte par la requête:

``` sql
GRANT SELECT(x,y) ON db.table TO john WITH GRANT OPTION
```

Cela signifie que `john` a la permission d'effectuer:

-   `SELECT x,y FROM db.table`.
-   `SELECT x FROM db.table`.
-   `SELECT y FROM db.table`.

`john` ne pouvez pas effectuer de `SELECT z FROM db.table`. Le `SELECT * FROM db.table` aussi n'est pas disponible. En traitant cette requête, ClickHouse ne renvoie aucune donnée, même `x` et `y`. La seule exception est si une table contient uniquement `x` et `y` colonnes, dans ce cas ClickHouse renvoie toutes les données.

Également `john` a l' `GRANT OPTION` privilège, de sorte qu'il peut accorder à d'autres utilisateurs avec des privilèges de la même ou de la plus petite portée.

Spécification des privilèges vous pouvez utiliser asterisk (`*`) au lieu d'une table ou d'un nom de base de données. Par exemple, l' `GRANT SELECT ON db.* TO john` requête permet `john` pour effectuer la `SELECT` requête sur toutes les tables dans `db` la base de données. En outre, vous pouvez omettre le nom de la base de données. Dans ce cas, des privilèges sont accordés pour la base de données actuelle, par exemple: `GRANT SELECT ON * TO john` accorde le privilège sur toutes les tables dans la base de données actuelle, `GRANT SELECT ON mytable TO john` accorde le privilège sur le `mytable` table dans la base de données actuelle.

L'accès à la `system` la base de données est toujours autorisée (puisque cette base de données est utilisée pour traiter les requêtes).

Vous pouvez accorder plusieurs privilèges à plusieurs comptes dans une requête. Requête `GRANT SELECT, INSERT ON *.* TO john, robin` permet de comptes `john` et `robin` pour effectuer la `INSERT` et `SELECT` des requêtes sur toutes les tables de toutes les bases de données sur le serveur.

## Privilège {#grant-privileges}

Privilège est une autorisation pour effectuer un type spécifique de requêtes.

Les privilèges ont une structure hiérarchique. Un ensemble de requêtes autorisées dépend de la portée des privilèges.

Hiérarchie des privilèges:

-   [SELECT](#grant-select)
-   [INSERT](#grant-insert)
-   [ALTER](#grant-alter)
    -   `ALTER TABLE`
        -   `ALTER UPDATE`
        -   `ALTER DELETE`
        -   `ALTER COLUMN`
            -   `ALTER ADD COLUMN`
            -   `ALTER DROP COLUMN`
            -   `ALTER MODIFY COLUMN`
            -   `ALTER COMMENT COLUMN`
            -   `ALTER CLEAR COLUMN`
            -   `ALTER RENAME COLUMN`
        -   `ALTER INDEX`
            -   `ALTER ORDER BY`
            -   `ALTER ADD INDEX`
            -   `ALTER DROP INDEX`
            -   `ALTER MATERIALIZE INDEX`
            -   `ALTER CLEAR INDEX`
        -   `ALTER CONSTRAINT`
            -   `ALTER ADD CONSTRAINT`
            -   `ALTER DROP CONSTRAINT`
        -   `ALTER TTL`
        -   `ALTER MATERIALIZE TTL`
        -   `ALTER SETTINGS`
        -   `ALTER MOVE PARTITION`
        -   `ALTER FETCH PARTITION`
        -   `ALTER FREEZE PARTITION`
    -   `ALTER VIEW`
        -   `ALTER VIEW REFRESH`
        -   `ALTER VIEW MODIFY QUERY`
-   [CREATE](#grant-create)
    -   `CREATE DATABASE`
    -   `CREATE TABLE`
    -   `CREATE VIEW`
    -   `CREATE DICTIONARY`
    -   `CREATE TEMPORARY TABLE`
-   [DROP](#grant-drop)
    -   `DROP DATABASE`
    -   `DROP TABLE`
    -   `DROP VIEW`
    -   `DROP DICTIONARY`
-   [TRUNCATE](#grant-truncate)
-   [OPTIMIZE](#grant-optimize)
-   [SHOW](#grant-show)
    -   `SHOW DATABASES`
    -   `SHOW TABLES`
    -   `SHOW COLUMNS`
    -   `SHOW DICTIONARIES`
-   [KILL QUERY](#grant-kill-query)
-   [ACCESS MANAGEMENT](#grant-access-management)
    -   `CREATE USER`
    -   `ALTER USER`
    -   `DROP USER`
    -   `CREATE ROLE`
    -   `ALTER ROLE`
    -   `DROP ROLE`
    -   `CREATE ROW POLICY`
    -   `ALTER ROW POLICY`
    -   `DROP ROW POLICY`
    -   `CREATE QUOTA`
    -   `ALTER QUOTA`
    -   `DROP QUOTA`
    -   `CREATE SETTINGS PROFILE`
    -   `ALTER SETTINGS PROFILE`
    -   `DROP SETTINGS PROFILE`
    -   `SHOW ACCESS`
        -   `SHOW_USERS`
        -   `SHOW_ROLES`
        -   `SHOW_ROW_POLICIES`
        -   `SHOW_QUOTAS`
        -   `SHOW_SETTINGS_PROFILES`
    -   `ROLE ADMIN`
-   [SYSTEM](#grant-system)
    -   `SYSTEM SHUTDOWN`
    -   `SYSTEM DROP CACHE`
        -   `SYSTEM DROP DNS CACHE`
        -   `SYSTEM DROP MARK CACHE`
        -   `SYSTEM DROP UNCOMPRESSED CACHE`
    -   `SYSTEM RELOAD`
        -   `SYSTEM RELOAD CONFIG`
        -   `SYSTEM RELOAD DICTIONARY`
        -   `SYSTEM RELOAD EMBEDDED DICTIONARIES`
    -   `SYSTEM MERGES`
    -   `SYSTEM TTL MERGES`
    -   `SYSTEM FETCHES`
    -   `SYSTEM MOVES`
    -   `SYSTEM SENDS`
        -   `SYSTEM DISTRIBUTED SENDS`
        -   `SYSTEM REPLICATED SENDS`
    -   `SYSTEM REPLICATION QUEUES`
    -   `SYSTEM SYNC REPLICA`
    -   `SYSTEM RESTART REPLICA`
    -   `SYSTEM FLUSH`
        -   `SYSTEM FLUSH DISTRIBUTED`
        -   `SYSTEM FLUSH LOGS`
-   [INTROSPECTION](#grant-introspection)
    -   `addressToLine`
    -   `addressToSymbol`
    -   `demangle`
-   [SOURCES](#grant-sources)
    -   `FILE`
    -   `URL`
    -   `REMOTE`
    -   `YSQL`
    -   `ODBC`
    -   `JDBC`
    -   `HDFS`
    -   `S3`
-   [dictGet](#grant-dictget)

Exemples de la façon dont cette hiérarchie est traitée:

-   Le `ALTER` privilège comprend tous les autres `ALTER*` privilège.
-   `ALTER CONSTRAINT` comprendre `ALTER ADD CONSTRAINT` et `ALTER DROP CONSTRAINT` privilège.

Les privilèges sont appliqués à différents niveaux. Connaissant un niveau suggère la syntaxe disponible pour le privilège.

Les niveaux (du plus faible au plus élevé):

-   `COLUMN` — Privilege can be granted for column, table, database, or globally.
-   `TABLE` — Privilege can be granted for table, database, or globally.
-   `VIEW` — Privilege can be granted for view, database, or globally.
-   `DICTIONARY` — Privilege can be granted for dictionary, database, or globally.
-   `DATABASE` — Privilege can be granted for database or globally.
-   `GLOBAL` — Privilege can be granted only globally.
-   `GROUP` — Groups privileges of different levels. When `GROUP`- le privilège de niveau est accordé, seuls les privilèges du groupe sont accordés qui correspondent à la syntaxe utilisée.

Exemples de syntaxe:

-   `GRANT SELECT(x) ON db.table TO user`
-   `GRANT SELECT ON db.* TO user`

Exemples de syntaxe refusée:

-   `GRANT CREATE USER(x) ON db.table TO user`
-   `GRANT CREATE USER ON db.* TO user`

Le privilège spécial [ALL](#grant-all) accorde tous les privilèges à un compte d'utilisateur ou à un rôle.

Par défaut, un compte d'utilisateur ou un rôle a pas de privilèges.

Si un utilisateur ou un rôle ont pas de privilèges qu'il s'affiche comme [NONE](#grant-none) privilège.

Certaines requêtes par leur implémentation nécessitent un ensemble de privilèges. Par exemple, pour effectuer la [RENAME](misc.md#misc_operations-rename) requête vous avez besoin des privilèges suivants: `SELECT`, `CREATE TABLE`, `INSERT` et `DROP TABLE`.

### SELECT {#grant-select}

Permet d'effectuer des [SELECT](select/index.md) requête.

Le niveau de privilège: `COLUMN`.

**Description**

L'utilisateur accordé avec ce privilège peut effectuer `SELECT` requêtes sur une liste spécifiée de colonnes dans la table et la base de données spécifiées. Si l'utilisateur inclut d'autres colonnes, une requête ne renvoie aucune donnée.

Considérez le privilège suivant:

``` sql
GRANT SELECT(x,y) ON db.table TO john
```

Ce privilège permet à `john` pour effectuer toute `SELECT` requête qui implique des données du `x` et/ou `y` les colonnes en `db.table`. Exemple, `SELECT x FROM db.table`. `john` ne pouvez pas effectuer de `SELECT z FROM db.table`. Le `SELECT * FROM db.table` aussi n'est pas disponible. En traitant cette requête, ClickHouse ne renvoie aucune donnée, même `x` et `y`. La seule exception est si une table contient uniquement `x` et `y` colonnes, dans ce cas ClickHouse renvoie toutes les données.

### INSERT {#grant-insert}

Permet d'effectuer des [INSERT](insert-into.md) requête.

Le niveau de privilège: `COLUMN`.

**Description**

L'utilisateur accordé avec ce privilège peut effectuer `INSERT` requêtes sur une liste spécifiée de colonnes dans la table et la base de données spécifiées. Si l'utilisateur inclut d'autres colonnes, une requête n'insère aucune donnée.

**Exemple**

``` sql
GRANT INSERT(x,y) ON db.table TO john
```

Le privilège accordé permet `john` pour insérer des données à l' `x` et/ou `y` les colonnes en `db.table`.

### ALTER {#grant-alter}

Permet d'effectuer des [ALTER](alter.md) requêtes correspondant à la hiérarchie de privilèges suivante:

-   `ALTER`. Niveau: `COLUMN`.
    -   `ALTER TABLE`. Niveau: `GROUP`
        -   `ALTER UPDATE`. Niveau: `COLUMN`. Alias: `UPDATE`
        -   `ALTER DELETE`. Niveau: `COLUMN`. Alias: `DELETE`
        -   `ALTER COLUMN`. Niveau: `GROUP`
            -   `ALTER ADD COLUMN`. Niveau: `COLUMN`. Alias: `ADD COLUMN`
            -   `ALTER DROP COLUMN`. Niveau: `COLUMN`. Alias: `DROP COLUMN`
            -   `ALTER MODIFY COLUMN`. Niveau: `COLUMN`. Alias: `MODIFY COLUMN`
            -   `ALTER COMMENT COLUMN`. Niveau: `COLUMN`. Alias: `COMMENT COLUMN`
            -   `ALTER CLEAR COLUMN`. Niveau: `COLUMN`. Alias: `CLEAR COLUMN`
            -   `ALTER RENAME COLUMN`. Niveau: `COLUMN`. Alias: `RENAME COLUMN`
        -   `ALTER INDEX`. Niveau: `GROUP`. Alias: `INDEX`
            -   `ALTER ORDER BY`. Niveau: `TABLE`. Alias: `ALTER MODIFY ORDER BY`, `MODIFY ORDER BY`
            -   `ALTER ADD INDEX`. Niveau: `TABLE`. Alias: `ADD INDEX`
            -   `ALTER DROP INDEX`. Niveau: `TABLE`. Alias: `DROP INDEX`
            -   `ALTER MATERIALIZE INDEX`. Niveau: `TABLE`. Alias: `MATERIALIZE INDEX`
            -   `ALTER CLEAR INDEX`. Niveau: `TABLE`. Alias: `CLEAR INDEX`
        -   `ALTER CONSTRAINT`. Niveau: `GROUP`. Alias: `CONSTRAINT`
            -   `ALTER ADD CONSTRAINT`. Niveau: `TABLE`. Alias: `ADD CONSTRAINT`
            -   `ALTER DROP CONSTRAINT`. Niveau: `TABLE`. Alias: `DROP CONSTRAINT`
        -   `ALTER TTL`. Niveau: `TABLE`. Alias: `ALTER MODIFY TTL`, `MODIFY TTL`
        -   `ALTER MATERIALIZE TTL`. Niveau: `TABLE`. Alias: `MATERIALIZE TTL`
        -   `ALTER SETTINGS`. Niveau: `TABLE`. Alias: `ALTER SETTING`, `ALTER MODIFY SETTING`, `MODIFY SETTING`
        -   `ALTER MOVE PARTITION`. Niveau: `TABLE`. Alias: `ALTER MOVE PART`, `MOVE PARTITION`, `MOVE PART`
        -   `ALTER FETCH PARTITION`. Niveau: `TABLE`. Alias: `FETCH PARTITION`
        -   `ALTER FREEZE PARTITION`. Niveau: `TABLE`. Alias: `FREEZE PARTITION`
    -   `ALTER VIEW` Niveau: `GROUP`
        -   `ALTER VIEW REFRESH`. Niveau: `VIEW`. Alias: `ALTER LIVE VIEW REFRESH`, `REFRESH VIEW`
        -   `ALTER VIEW MODIFY QUERY`. Niveau: `VIEW`. Alias: `ALTER TABLE MODIFY QUERY`

Exemples de la façon dont cette hiérarchie est traitée:

-   Le `ALTER` privilège comprend tous les autres `ALTER*` privilège.
-   `ALTER CONSTRAINT` comprendre `ALTER ADD CONSTRAINT` et `ALTER DROP CONSTRAINT` privilège.

**Note**

-   Le `MODIFY SETTING` privilège permet de modifier les paramètres du moteur de table. In n'affecte pas les paramètres ou les paramètres de configuration du serveur.
-   Le `ATTACH` opération a besoin de la [CREATE](#grant-create) privilège.
-   Le `DETACH` opération a besoin de la [DROP](#grant-drop) privilège.
-   Pour arrêter la mutation par le [KILL MUTATION](misc.md#kill-mutation) requête, vous devez avoir un privilège pour commencer cette mutation. Par exemple, si vous voulez arrêter l' `ALTER UPDATE` requête, vous avez besoin du `ALTER UPDATE`, `ALTER TABLE`, ou `ALTER` privilège.

### CREATE {#grant-create}

Permet d'effectuer des [CREATE](create.md) et [ATTACH](misc.md#attach) DDL-requêtes correspondant à la hiérarchie de privilèges suivante:

-   `CREATE`. Niveau: `GROUP`
    -   `CREATE DATABASE`. Niveau: `DATABASE`
    -   `CREATE TABLE`. Niveau: `TABLE`
    -   `CREATE VIEW`. Niveau: `VIEW`
    -   `CREATE DICTIONARY`. Niveau: `DICTIONARY`
    -   `CREATE TEMPORARY TABLE`. Niveau: `GLOBAL`

**Note**

-   Pour supprimer la table créée, l'utilisateur doit [DROP](#grant-drop).

### DROP {#grant-drop}

Permet d'effectuer des [DROP](misc.md#drop) et [DETACH](misc.md#detach) requêtes correspondant à la hiérarchie de privilèges suivante:

-   `DROP`. Niveau:
    -   `DROP DATABASE`. Niveau: `DATABASE`
    -   `DROP TABLE`. Niveau: `TABLE`
    -   `DROP VIEW`. Niveau: `VIEW`
    -   `DROP DICTIONARY`. Niveau: `DICTIONARY`

### TRUNCATE {#grant-truncate}

Permet d'effectuer des [TRUNCATE](misc.md#truncate-statement) requête.

Le niveau de privilège: `TABLE`.

### OPTIMIZE {#grant-optimize}

Permet d'effectuer les [OPTIMIZE TABLE](misc.md#misc_operations-optimize) requête.

Le niveau de privilège: `TABLE`.

### SHOW {#grant-show}

Permet d'effectuer des `SHOW`, `DESCRIBE`, `USE`, et `EXISTS` requêtes, correspondant à la hiérarchie suivante des privilèges:

-   `SHOW`. Niveau: `GROUP`
    -   `SHOW DATABASES`. Niveau: `DATABASE`. Permet d'exécuter des `SHOW DATABASES`, `SHOW CREATE DATABASE`, `USE <database>` requête.
    -   `SHOW TABLES`. Niveau: `TABLE`. Permet d'exécuter des `SHOW TABLES`, `EXISTS <table>`, `CHECK <table>` requête.
    -   `SHOW COLUMNS`. Niveau: `COLUMN`. Permet d'exécuter des `SHOW CREATE TABLE`, `DESCRIBE` requête.
    -   `SHOW DICTIONARIES`. Niveau: `DICTIONARY`. Permet d'exécuter des `SHOW DICTIONARIES`, `SHOW CREATE DICTIONARY`, `EXISTS <dictionary>` requête.

**Note**

Un utilisateur a le `SHOW` privilège s'il a un autre privilège concernant la table, le dictionnaire ou la base de données spécifiés.

### KILL QUERY {#grant-kill-query}

Permet d'effectuer les [KILL](misc.md#kill-query-statement) requêtes correspondant à la hiérarchie de privilèges suivante:

Le niveau de privilège: `GLOBAL`.

**Note**

`KILL QUERY` privilège permet à un utilisateur de tuer les requêtes des autres utilisateurs.

### ACCESS MANAGEMENT {#grant-access-management}

Permet à un utilisateur d'effectuer des requêtes qui gèrent les utilisateurs, les rôles et les stratégies de ligne.

-   `ACCESS MANAGEMENT`. Niveau: `GROUP`
    -   `CREATE USER`. Niveau: `GLOBAL`
    -   `ALTER USER`. Niveau: `GLOBAL`
    -   `DROP USER`. Niveau: `GLOBAL`
    -   `CREATE ROLE`. Niveau: `GLOBAL`
    -   `ALTER ROLE`. Niveau: `GLOBAL`
    -   `DROP ROLE`. Niveau: `GLOBAL`
    -   `ROLE ADMIN`. Niveau: `GLOBAL`
    -   `CREATE ROW POLICY`. Niveau: `GLOBAL`. Alias: `CREATE POLICY`
    -   `ALTER ROW POLICY`. Niveau: `GLOBAL`. Alias: `ALTER POLICY`
    -   `DROP ROW POLICY`. Niveau: `GLOBAL`. Alias: `DROP POLICY`
    -   `CREATE QUOTA`. Niveau: `GLOBAL`
    -   `ALTER QUOTA`. Niveau: `GLOBAL`
    -   `DROP QUOTA`. Niveau: `GLOBAL`
    -   `CREATE SETTINGS PROFILE`. Niveau: `GLOBAL`. Alias: `CREATE PROFILE`
    -   `ALTER SETTINGS PROFILE`. Niveau: `GLOBAL`. Alias: `ALTER PROFILE`
    -   `DROP SETTINGS PROFILE`. Niveau: `GLOBAL`. Alias: `DROP PROFILE`
    -   `SHOW ACCESS`. Niveau: `GROUP`
        -   `SHOW_USERS`. Niveau: `GLOBAL`. Alias: `SHOW CREATE USER`
        -   `SHOW_ROLES`. Niveau: `GLOBAL`. Alias: `SHOW CREATE ROLE`
        -   `SHOW_ROW_POLICIES`. Niveau: `GLOBAL`. Alias: `SHOW POLICIES`, `SHOW CREATE ROW POLICY`, `SHOW CREATE POLICY`
        -   `SHOW_QUOTAS`. Niveau: `GLOBAL`. Alias: `SHOW CREATE QUOTA`
        -   `SHOW_SETTINGS_PROFILES`. Niveau: `GLOBAL`. Alias: `SHOW PROFILES`, `SHOW CREATE SETTINGS PROFILE`, `SHOW CREATE PROFILE`

Le `ROLE ADMIN` le privilège permet à un utilisateur d'accorder et de révoquer tous les rôles, y compris ceux qui ne lui sont pas accordés avec l'option admin.

### SYSTEM {#grant-system}

Permet à un utilisateur d'effectuer la [SYSTEM](system.md) requêtes correspondant à la hiérarchie de privilèges suivante.

-   `SYSTEM`. Niveau: `GROUP`
    -   `SYSTEM SHUTDOWN`. Niveau: `GLOBAL`. Alias: `SYSTEM KILL`, `SHUTDOWN`
    -   `SYSTEM DROP CACHE`. Alias: `DROP CACHE`
        -   `SYSTEM DROP DNS CACHE`. Niveau: `GLOBAL`. Alias: `SYSTEM DROP DNS`, `DROP DNS CACHE`, `DROP DNS`
        -   `SYSTEM DROP MARK CACHE`. Niveau: `GLOBAL`. Alias: `SYSTEM DROP MARK`, `DROP MARK CACHE`, `DROP MARKS`
        -   `SYSTEM DROP UNCOMPRESSED CACHE`. Niveau: `GLOBAL`. Alias: `SYSTEM DROP UNCOMPRESSED`, `DROP UNCOMPRESSED CACHE`, `DROP UNCOMPRESSED`
    -   `SYSTEM RELOAD`. Niveau: `GROUP`
        -   `SYSTEM RELOAD CONFIG`. Niveau: `GLOBAL`. Alias: `RELOAD CONFIG`
        -   `SYSTEM RELOAD DICTIONARY`. Niveau: `GLOBAL`. Alias: `SYSTEM RELOAD DICTIONARIES`, `RELOAD DICTIONARY`, `RELOAD DICTIONARIES`
        -   `SYSTEM RELOAD EMBEDDED DICTIONARIES`. Niveau: `GLOBAL`. Alias: R`ELOAD EMBEDDED DICTIONARIES`
    -   `SYSTEM MERGES`. Niveau: `TABLE`. Alias: `SYSTEM STOP MERGES`, `SYSTEM START MERGES`, `STOP MERGES`, `START MERGES`
    -   `SYSTEM TTL MERGES`. Niveau: `TABLE`. Alias: `SYSTEM STOP TTL MERGES`, `SYSTEM START TTL MERGES`, `STOP TTL MERGES`, `START TTL MERGES`
    -   `SYSTEM FETCHES`. Niveau: `TABLE`. Alias: `SYSTEM STOP FETCHES`, `SYSTEM START FETCHES`, `STOP FETCHES`, `START FETCHES`
    -   `SYSTEM MOVES`. Niveau: `TABLE`. Alias: `SYSTEM STOP MOVES`, `SYSTEM START MOVES`, `STOP MOVES`, `START MOVES`
    -   `SYSTEM SENDS`. Niveau: `GROUP`. Alias: `SYSTEM STOP SENDS`, `SYSTEM START SENDS`, `STOP SENDS`, `START SENDS`
        -   `SYSTEM DISTRIBUTED SENDS`. Niveau: `TABLE`. Alias: `SYSTEM STOP DISTRIBUTED SENDS`, `SYSTEM START DISTRIBUTED SENDS`, `STOP DISTRIBUTED SENDS`, `START DISTRIBUTED SENDS`
        -   `SYSTEM REPLICATED SENDS`. Niveau: `TABLE`. Alias: `SYSTEM STOP REPLICATED SENDS`, `SYSTEM START REPLICATED SENDS`, `STOP REPLICATED SENDS`, `START REPLICATED SENDS`
    -   `SYSTEM REPLICATION QUEUES`. Niveau: `TABLE`. Alias: `SYSTEM STOP REPLICATION QUEUES`, `SYSTEM START REPLICATION QUEUES`, `STOP REPLICATION QUEUES`, `START REPLICATION QUEUES`
    -   `SYSTEM SYNC REPLICA`. Niveau: `TABLE`. Alias: `SYNC REPLICA`
    -   `SYSTEM RESTART REPLICA`. Niveau: `TABLE`. Alias: `RESTART REPLICA`
    -   `SYSTEM FLUSH`. Niveau: `GROUP`
        -   `SYSTEM FLUSH DISTRIBUTED`. Niveau: `TABLE`. Alias: `FLUSH DISTRIBUTED`
        -   `SYSTEM FLUSH LOGS`. Niveau: `GLOBAL`. Alias: `FLUSH LOGS`

Le `SYSTEM RELOAD EMBEDDED DICTIONARIES` privilège implicitement accordé par le `SYSTEM RELOAD DICTIONARY ON *.*` privilège.

### INTROSPECTION {#grant-introspection}

Permet l'utilisation de [introspection](../../operations/optimizing-performance/sampling-query-profiler.md) fonction.

-   `INTROSPECTION`. Niveau: `GROUP`. Alias: `INTROSPECTION FUNCTIONS`
    -   `addressToLine`. Niveau: `GLOBAL`
    -   `addressToSymbol`. Niveau: `GLOBAL`
    -   `demangle`. Niveau: `GLOBAL`

### SOURCES {#grant-sources}

Permet d'utiliser des sources de données externes. S'applique à [moteurs de table](../../engines/table-engines/index.md) et [les fonctions de table](../table-functions/index.md#table-functions).

-   `SOURCES`. Niveau: `GROUP`
    -   `FILE`. Niveau: `GLOBAL`
    -   `URL`. Niveau: `GLOBAL`
    -   `REMOTE`. Niveau: `GLOBAL`
    -   `YSQL`. Niveau: `GLOBAL`
    -   `ODBC`. Niveau: `GLOBAL`
    -   `JDBC`. Niveau: `GLOBAL`
    -   `HDFS`. Niveau: `GLOBAL`
    -   `S3`. Niveau: `GLOBAL`

Le `SOURCES` privilège permet l'utilisation de toutes les sources. Vous pouvez également accorder un privilège pour chaque source individuellement. Pour utiliser les sources, vous avez besoin de privilèges supplémentaires.

Exemple:

-   Pour créer une table avec [Moteur de table MySQL](../../engines/table-engines/integrations/mysql.md), vous avez besoin `CREATE TABLE (ON db.table_name)` et `MYSQL` privilège.
-   L'utilisation de la [fonction de table mysql](../table-functions/mysql.md), vous avez besoin `CREATE TEMPORARY TABLE` et `MYSQL` privilège.

### dictGet {#grant-dictget}

-   `dictGet`. Alias: `dictHas`, `dictGetHierarchy`, `dictIsIn`

Permet à un utilisateur d'exécuter [dictGet](../functions/ext-dict-functions.md#dictget), [dictHas](../functions/ext-dict-functions.md#dicthas), [dictGetHierarchy](../functions/ext-dict-functions.md#dictgethierarchy), [dictisine](../functions/ext-dict-functions.md#dictisin) fonction.

Niveau de privilège: `DICTIONARY`.

**Exemple**

-   `GRANT dictGet ON mydb.mydictionary TO john`
-   `GRANT dictGet ON mydictionary TO john`

### ALL {#grant-all}

Les subventions de tous les privilèges sur l'entité réglementée à un compte d'utilisateur ou un rôle.

### NONE {#grant-none}

N'accorde pas de privilèges.

### ADMIN OPTION {#admin-option-privilege}

Le `ADMIN OPTION` le privilège permet à un utilisateur d'accorder son rôle à un autre utilisateur.

[Article Original](https://clickhouse.tech/docs/en/query_language/grant/) <!--hide-->
