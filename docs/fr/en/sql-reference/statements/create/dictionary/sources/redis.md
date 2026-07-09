---
slug: /sql-reference/statements/create/dictionary/sources/redis
title: 'Source Redis pour dictionnaire'
sidebar_position: 10
sidebar_label: 'Redis'
description: 'Configurer Redis comme source de dictionnaire dans ClickHouse.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Exemple de paramètres :

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(REDIS(
        host 'localhost'
        port 6379
        storage_type 'simple'
        db_index 0
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="Fichier de configuration">
    ```xml
    <source>
        <redis>
            <host>localhost</host>
            <port>6379</port>
            <storage_type>simple</storage_type>
            <db_index>0</db_index>
        </redis>
    </source>
    ```
  </TabItem>
</Tabs>

<br />

Champs des paramètres :

| Paramètre      | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `host`         | L’hôte de Redis.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `port`         | Le port du serveur Redis.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| `storage_type` | La structure du stockage interne de Redis utilisée pour manipuler les clés. `simple` utilise un mappage clé-valeur plat et prend en charge les layouts à clé simple ainsi que les layouts à clé complexe sur une seule colonne (comme `complex_key_cache` et `complex_key_direct`). `hash_map` utilise un hash Redis et est requis pour les clés complexes composées ; il attend exactement deux colonnes de clé. Les colonnes de clé doivent être de type entier ou chaîne de caractères. Les layouts par plage ne sont pas pris en charge. La valeur par défaut est `simple`. Facultatif. |
| `db_index`     | L’index numérique spécifique de la base de données logique Redis. La valeur par défaut est `0`. Facultatif.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |