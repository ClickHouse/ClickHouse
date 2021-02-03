---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 20
toc_title: Interface MySQL
---

# Interface MySQL {#mysql-interface}

ClickHouse prend en charge le protocole de fil MySQL. Il peut être activé par [mysql_port](../operations/server-configuration-parameters/settings.md#server_configuration_parameters-mysql_port) paramètre dans le fichier de configuration:

``` xml
<mysql_port>9004</mysql_port>
```

Exemple de connexion à l'aide d'outil de ligne de commande `mysql`:

``` bash
$ mysql --protocol tcp -u default -P 9004
```

Sortie si une connexion a réussi:

``` text
Welcome to the MySQL monitor.  Commands end with ; or \g.
Your MySQL connection id is 4
Server version: 20.2.1.1-ClickHouse

Copyright (c) 2000, 2019, Oracle and/or its affiliates. All rights reserved.

Oracle is a registered trademark of Oracle Corporation and/or its
affiliates. Other names may be trademarks of their respective
owners.

Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

mysql>
```

Pour la compatibilité avec tous les clients MySQL, il est recommandé de spécifier le mot de passe utilisateur avec [double SHA1](../operations/settings/settings-users.md#password_double_sha1_hex) dans le fichier de configuration.
Si le mot de passe de l'utilisateur est spécifié [SHA256](../operations/settings/settings-users.md#password_sha256_hex), certains clients ne pourront pas s'authentifier (mysqljs et anciennes versions de l'outil de ligne de commande mysql).

Restriction:

-   les requêtes préparées ne sont pas prises en charge

-   certains types de données sont envoyés sous forme de chaînes

[Article Original](https://clickhouse.tech/docs/en/interfaces/mysql/) <!--hide-->
