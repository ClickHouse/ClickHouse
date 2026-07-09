---
slug: /sql-reference/statements/create/dictionary/sources/postgresql
title: 'Source de dictionnaire PostgreSQL'
sidebar_position: 12
sidebar_label: 'PostgreSQL'
description: 'Configurer PostgreSQL comme source de dictionnaire dans ClickHouse.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Exemple de paramètres :

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(POSTGRESQL(
        port 5432
        host 'postgresql-hostname'
        user 'postgres_user'
        password 'postgres_password'
        db 'db_name'
        table 'table_name'
        replica(host 'example01-1' port 5432 priority 1)
        replica(host 'example01-2' port 5432 priority 2)
        where 'id=10'
        invalidate_query 'SQL_QUERY'
        query 'SELECT id, value_1, value_2 FROM db_name.table_name'
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="Fichier de configuration">
    ```xml
    <source>
      <postgresql>
          <host>postgresql-hostname</hoat>
          <port>5432</port>
          <user>clickhouse</user>
          <password>qwerty</password>
          <db>db_name</db>
          <table>table_name</table>
          <where>id=10</where>
          <invalidate_query>SQL_QUERY</invalidate_query>
          <query>SELECT id, value_1, value_2 FROM db_name.table_name</query>
      </postgresql>
    </source>
    ```
  </TabItem>
</Tabs>

<br />

Champs de paramétrage :

| Paramètre              | Description                                                                                                                                                                                    |
| ---------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `host`                 | L’hôte du serveur PostgreSQL. Vous pouvez le spécifier pour toutes les répliques ou pour chacune individuellement (dans `<replica>`).                                                          |
| `port`                 | Le port du serveur PostgreSQL. Vous pouvez le spécifier pour toutes les répliques ou pour chacune individuellement (dans `<replica>`).                                                         |
| `user`                 | Nom de l’utilisateur PostgreSQL. Vous pouvez le spécifier pour toutes les répliques ou pour chacune individuellement (dans `<replica>`).                                                       |
| `password`             | Mot de passe de l’utilisateur PostgreSQL. Vous pouvez le spécifier pour toutes les répliques ou pour chacune individuellement (dans `<replica>`).                                              |
| `replica`              | Section de configuration des répliques. Il peut y avoir plusieurs sections.                                                                                                                    |
| `replica/host`         | L’hôte PostgreSQL.                                                                                                                                                                             |
| `replica/port`         | Le port PostgreSQL.                                                                                                                                                                            |
| `replica/priority`     | Priorité de la réplique. Lors d’une tentative de connexion, ClickHouse parcourt les répliques par ordre de priorité. Plus le nombre est faible, plus la priorité est élevée.                   |
| `db`                   | Nom de la base de données.                                                                                                                                                                     |
| `table`                | Nom de la table.                                                                                                                                                                               |
| `where`                | Critère de sélection. La syntaxe des conditions est la même que celle de la clause `WHERE` dans PostgreSQL. Par exemple, `id > 10 AND id < 20`. Facultatif.                                    |
| `invalidate_query`     | Requête permettant de vérifier l’état du dictionnaire. Facultatif. Pour en savoir plus, consultez la section [Actualisation des données du dictionnaire à l’aide de LIFETIME](../lifetime.md). |
| `background_reconnect` | Se reconnecte à une réplique en arrière-plan si la connexion échoue. Facultatif.                                                                                                               |
| `query`                | Requête personnalisée. Facultatif.                                                                                                                                                             |

:::note
Les champs `table` et `where` ne peuvent pas être utilisés avec le champ `query`. De plus, l’un des champs `table` ou `query` doit obligatoirement être déclaré.
:::