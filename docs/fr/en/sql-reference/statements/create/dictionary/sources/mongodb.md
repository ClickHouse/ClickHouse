---
slug: /sql-reference/statements/create/dictionary/sources/mongodb
title: 'Source MongoDB pour dictionnaire'
sidebar_position: 9
sidebar_label: 'MongoDB'
description: 'Configurer MongoDB comme source de dictionnaire dans ClickHouse.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Exemple de paramètres :

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(MONGODB(
        host 'localhost'
        port 27017
        user ''
        password ''
        db 'test'
        collection 'dictionary_source'
        options 'ssl=true'
    ))
    ```

    Ou avec un URI :

    ```sql
    SOURCE(MONGODB(
        uri 'mongodb://localhost:27017/clickhouse'
        collection 'dictionary_source'
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="Fichier de configuration">
    ```xml
    <source>
        <mongodb>
            <host>localhost</host>
            <port>27017</port>
            <user></user>
            <password></password>
            <db>test</db>
            <collection>dictionary_source</collection>
            <options>ssl=true</options>
        </mongodb>
    </source>
    ```

    Ou avec un URI :

    ```xml
    <source>
        <mongodb>
            <uri>mongodb://localhost:27017/test?ssl=true</uri>
            <collection>dictionary_source</collection>
        </mongodb>
    </source>
    ```
  </TabItem>
</Tabs>

<br />

Champs des paramètres :

| Paramètre    | Description                                                                              |
| ------------ | ---------------------------------------------------------------------------------------- |
| `host`       | L’hôte MongoDB.                                                                          |
| `port`       | Le port du serveur MongoDB.                                                              |
| `user`       | Nom de l’utilisateur MongoDB.                                                            |
| `password`   | Mot de passe de l’utilisateur MongoDB.                                                   |
| `db`         | Nom de la base de données.                                                               |
| `collection` | Nom de la collection.                                                                    |
| `options`    | Options de la chaîne de connexion MongoDB. Optionnel.                                    |
| `uri`        | URI permettant d’établir la connexion (alternative aux champs individuels host/port/db). |

[Plus d’informations sur le moteur](/fr/engines/table-engines/integrations/mongodb)