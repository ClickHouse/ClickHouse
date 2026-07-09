---
slug: /sql-reference/statements/create/dictionary/sources/clickhouse
title: 'Source de dictionnaire ClickHouse'
sidebar_position: 8
sidebar_label: 'ClickHouse'
description: 'Configurer une table ClickHouse comme source de dictionnaire.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Exemple de paramètres :

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(CLICKHOUSE(
        host 'example01-01-1'
        port 9000
        user 'default'
        password ''
        db 'default'
        table 'ids'
        where 'id=10'
        secure 1
        query 'SELECT id, value_1, value_2 FROM default.ids'
    ));
    ```
  </TabItem>

  <TabItem value="xml" label="Fichier de configuration">
    ```xml
    <source>
        <clickhouse>
            <host>example01-01-1</host>
            <port>9000</port>
            <user>default</user>
            <password></password>
            <db>default</db>
            <table>ids</table>
            <where>id=10</where>
            <secure>1</secure>
            <query>SELECT id, value_1, value_2 FROM default.ids</query>
        </clickhouse>
    </source>
    ```
  </TabItem>
</Tabs>

<br />

Champs des paramètres :

| Paramètre          | Description                                                                                                                                                                                                                                                                          |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `host`             | L’hôte ClickHouse. S’il s’agit d’un hôte local, la requête est traitée sans aucune activité réseau. Pour améliorer la tolérance aux pannes, vous pouvez créer une table [Distributed](/fr/engines/table-engines/special/distributed) et l’indiquer dans des configurations ultérieures. |
| `port`             | Le port du serveur ClickHouse.                                                                                                                                                                                                                                                       |
| `user`             | Nom de l’utilisateur ClickHouse.                                                                                                                                                                                                                                                     |
| `password`         | Mot de passe de l’utilisateur ClickHouse.                                                                                                                                                                                                                                            |
| `db`               | Nom de la base de données.                                                                                                                                                                                                                                                           |
| `table`            | Nom de la table.                                                                                                                                                                                                                                                                     |
| `where`            | Les critères de sélection. Facultatif.                                                                                                                                                                                                                                               |
| `invalidate_query` | Requête permettant de vérifier l’état du dictionnaire. Facultatif. Pour en savoir plus, consultez la section [Actualisation des données du dictionnaire à l’aide de LIFETIME](../lifetime.md).                                                                                       |
| `secure`           | Utilisez SSL pour la connexion.                                                                                                                                                                                                                                                      |
| `query`            | La requête personnalisée. Facultatif.                                                                                                                                                                                                                                                |

:::note
Les champs `table` ou `where` ne peuvent pas être utilisés avec le champ `query`. De plus, l’un des champs `table` ou `query` doit être déclaré.
:::