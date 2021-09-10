---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 43
toc_title: jdbc
---

# jdbc {#table-function-jdbc}

`jdbc(jdbc_connection_uri, schema, table)` - retourne la table qui est connectée via le pilote JDBC.

Ce tableau fonction nécessite séparé `clickhouse-jdbc-bridge` programme en cours d'exécution.
Il prend en charge les types Nullable (basé sur DDL de la table distante qui est interrogée).

**Exemple**

``` sql
SELECT * FROM jdbc('jdbc:mysql://localhost:3306/?user=root&password=root', 'schema', 'table')
```

``` sql
SELECT * FROM jdbc('mysql://localhost:3306/?user=root&password=root', 'schema', 'table')
```

``` sql
SELECT * FROM jdbc('datasource://mysql-local', 'schema', 'table')
```

[Article Original](https://clickhouse.tech/docs/en/query_language/table_functions/jdbc/) <!--hide-->
