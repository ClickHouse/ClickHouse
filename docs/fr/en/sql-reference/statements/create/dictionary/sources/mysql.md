---
slug: /sql-reference/statements/create/dictionary/sources/mysql
title: 'Source de dictionnaire MySQL'
sidebar_position: 7
sidebar_label: 'MySQL'
description: 'Configurez MySQL comme source de dictionnaire dans ClickHouse.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Exemple de paramètres :

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(MYSQL(
        port 3306
        user 'clickhouse'
        password 'qwerty'
        replica(host 'example01-1' priority 1)
        replica(host 'example01-2' priority 1)
        db 'db_name'
        table 'table_name'
        where 'id=10'
        invalidate_query 'SQL_QUERY'
        fail_on_connection_loss 'true'
        query 'SELECT id, value_1, value_2 FROM db_name.table_name'
        enable_compression 1
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="Fichier de configuration">
    ```xml
    <source>
      <mysql>
          <port>3306</port>
          <user>clickhouse</user>
          <password>qwerty</password>
          <replica>
              <host>example01-1</host>
              <priority>1</priority>
          </replica>
          <replica>
              <host>example01-2</host>
              <priority>1</priority>
          </replica>
          <db>db_name</db>
          <table>table_name</table>
          <where>id=10</where>
          <invalidate_query>SQL_QUERY</invalidate_query>
          <fail_on_connection_loss>true</fail_on_connection_loss>
          <query>SELECT id, value_1, value_2 FROM db_name.table_name</query>
          <enable_compression>1</enable_compression>
      </mysql>
    </source>
    ```
  </TabItem>
</Tabs>

<br />

Champs des paramètres :

| Paramètre                 | Description                                                                                                                                                                                                                                                                                                                                                                              |
| ------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `port`                    | Le port du serveur MySQL. Vous pouvez le spécifier pour toutes les répliques ou pour chacune individuellement (dans `<replica>`).                                                                                                                                                                                                                                                        |
| `user`                    | Nom de l’utilisateur MySQL. Vous pouvez le spécifier pour toutes les répliques ou pour chacune individuellement (dans `<replica>`).                                                                                                                                                                                                                                                      |
| `password`                | Mot de passe de l’utilisateur MySQL. Vous pouvez le spécifier pour toutes les répliques ou pour chacune individuellement (dans `<replica>`).                                                                                                                                                                                                                                             |
| `replica`                 | Section de configuration des répliques. Il peut y avoir plusieurs sections.                                                                                                                                                                                                                                                                                                              |
| `replica/host`            | L’hôte MySQL.                                                                                                                                                                                                                                                                                                                                                                            |
| `replica/priority`        | La priorité de la réplique. Lors d’une tentative de connexion, ClickHouse parcourt les répliques par ordre de priorité. Plus le nombre est faible, plus la priorité est élevée.                                                                                                                                                                                                          |
| `db`                      | Nom de la base de données.                                                                                                                                                                                                                                                                                                                                                               |
| `table`                   | Nom de la table.                                                                                                                                                                                                                                                                                                                                                                         |
| `where`                   | Les critères de sélection. La syntaxe des conditions est la même que pour la clause `WHERE` dans MySQL, par exemple `id > 10 AND id < 20`. Facultatif.                                                                                                                                                                                                                                   |
| `invalidate_query`        | Query permettant de vérifier l’état du dictionnaire. Facultatif. Pour en savoir plus, consultez la section [Refreshing dictionary data using LIFETIME](../lifetime.md).                                                                                                                                                                                                                  |
| `fail_on_connection_loss` | Contrôle le comportement du serveur en cas de perte de connexion. Si `true`, une exception est immédiatement levée si la connexion entre le client et le serveur est perdue. Si `false`, le serveur réessaie de récupérer les données au moins trois fois avant de signaler une erreur. Notez que ces nouvelles tentatives augmentent les temps de réponse. Valeur par défaut : `false`. |
| `query`                   | La query personnalisée. Facultatif.                                                                                                                                                                                                                                                                                                                                                      |
| `enable_compression`      | Active la compression zlib pour la connexion du protocole MySQL. Lorsqu’elle est définie sur `1`, ClickHouse demande une compression au niveau du protocole au serveur MySQL. Peut également être définie pour chaque réplique dans `<replica>`. Valeur par défaut : `0`.                                                                                                                |

:::note
Les champs `table` ou `where` ne peuvent pas être utilisés avec le champ `query`. L’un des champs `table` ou `query` doit obligatoirement être déclaré.
:::

:::note
Il n’existe pas de paramètre explicite `secure`. Lors de l’établissement d’une connexion SSL, la sécurité est obligatoire.
:::

Il est possible de se connecter à MySQL sur l’hôte local via des sockets. Pour cela, définissez `host` et `socket`.

Exemple de paramètres :

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(MYSQL(
        host 'localhost'
        socket '/path/to/socket/file.sock'
        user 'clickhouse'
        password 'qwerty'
        db 'db_name'
        table 'table_name'
        where 'id=10'
        invalidate_query 'SQL_QUERY'
        fail_on_connection_loss 'true'
        query 'SELECT id, value_1, value_2 FROM db_name.table_name'
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="Fichier de configuration">
    ```xml
    <source>
      <mysql>
          <host>localhost</host>
          <socket>/path/to/socket/file.sock</socket>
          <user>clickhouse</user>
          <password>qwerty</password>
          <db>db_name</db>
          <table>table_name</table>
          <where>id=10</where>
          <invalidate_query>SQL_QUERY</invalidate_query>
          <fail_on_connection_loss>true</fail_on_connection_loss>
          <query>SELECT id, value_1, value_2 FROM db_name.table_name</query>
      </mysql>
    </source>
    ```
  </TabItem>
</Tabs>