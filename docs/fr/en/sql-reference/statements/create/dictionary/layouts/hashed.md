---
slug: /sql-reference/statements/create/dictionary/layouts/hashed
title: 'types de layout de dictionnaire hashed'
sidebar_label: 'hashed'
sidebar_position: 3
description: 'Stocke un dictionnaire en mémoire à l’aide de tables de hachage : hashed, sparse_hashed, complex_key_hashed, complex_key_sparse_hashed'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="hashed">
  ## hashed
</div>

Le dictionnaire est entièrement stocké en mémoire sous la forme d’une table de hachage. Il peut contenir autant d’éléments que nécessaire, avec n’importe quels identifiants. En pratique, le nombre de clés peut atteindre plusieurs dizaines de millions.

La clé du dictionnaire est de type [UInt64](/fr/sql-reference/data-types/int-uint.md).

Tous les types de sources sont pris en charge. Lors d’une mise à jour, les données (depuis un fichier ou une table) sont lues dans leur intégralité.

Exemple de configuration :

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(HASHED())
    ```
  </TabItem>

  <TabItem value="xml" label="Fichier de configuration">
    ```xml
    <layout>
      <hashed />
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

Exemple de configuration avec paramètres :

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(HASHED([SHARDS 1] [SHARD_LOAD_QUEUE_BACKLOG 10000] [MAX_LOAD_FACTOR 0.5]))
    ```
  </TabItem>

  <TabItem value="xml" label="Fichier de configuration">
    ```xml
    <layout>
      <hashed>
        <!-- Si shards est supérieur à 1 (la valeur par défaut est `1`), le dictionnaire chargera
             les données en parallèle, ce qui est utile si vous avez un très grand nombre d’éléments dans un
             dictionnaire. -->
        <shards>10</shards>

        <!-- Taille du tampon pour les blocs dans la file d’attente parallèle.

             Étant donné que le goulot d’étranglement du chargement en parallèle se situe au niveau du rehachage,
             il faut prévoir un certain tampon afin d’éviter les blocages lorsqu’un thread effectue le rehachage.

             10000 offre un bon compromis entre mémoire et vitesse.
             Même avec 10e10 éléments, cette valeur permet d’absorber toute la charge sans laisser les threads à court de travail. -->
        <shard_load_queue_backlog>10000</shard_load_queue_backlog>

        <!-- Facteur de charge maximal de la table de hachage : avec des valeurs plus élevées, la mémoire
             est utilisée plus efficacement (moins de mémoire est gaspillée), mais les performances de lecture
             peuvent se dégrader.

             Valeurs valides : [0.5, 0.99]
             Par défaut : 0.5 -->
        <max_load_factor>0.5</max_load_factor>
      </hashed>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

<div id="sparse_hashed">
  ## sparse_hashed
</div>

Semblable à `hashed`, mais utilise moins de mémoire au prix d’une utilisation CPU plus élevée.

La clé du dictionnaire est de type [UInt64](/fr/sql-reference/data-types/int-uint.md).

Exemple de configuration :

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(SPARSE_HASHED([SHARDS 1] [SHARD_LOAD_QUEUE_BACKLOG 10000] [MAX_LOAD_FACTOR 0.5]))
    ```
  </TabItem>

  <TabItem value="xml" label="Fichier de configuration">
    ```xml
    <layout>
      <sparse_hashed>
        <!-- <shards>1</shards> -->
        <!-- <shard_load_queue_backlog>10000</shard_load_queue_backlog> -->
        <!-- <max_load_factor>0.5</max_load_factor> -->
      </sparse_hashed>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

Il est également possible d’utiliser `shards` pour ce type de dictionnaire, et c’est encore plus important pour `sparse_hashed` que pour `hashed`, puisque `sparse_hashed` est plus lent.

<div id="complex_key_hashed">
  ## complex_key_hashed
</div>

Ce type de stockage s’utilise avec des [clés](../attributes.md#composite-key) composites. Similaire à `hashed`.

Exemple de configuration :

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(COMPLEX_KEY_HASHED([SHARDS 1] [SHARD_LOAD_QUEUE_BACKLOG 10000] [MAX_LOAD_FACTOR 0.5]))
    ```
  </TabItem>

  <TabItem value="xml" label="Fichier de configuration">
    ```xml
    <layout>
      <complex_key_hashed>
        <!-- <shards>1</shards> -->
        <!-- <shard_load_queue_backlog>10000</shard_load_queue_backlog> -->
        <!-- <max_load_factor>0.5</max_load_factor> -->
      </complex_key_hashed>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

<div id="complex_key_sparse_hashed">
  ## complex_key_sparse_hashed
</div>

Ce type de stockage s’utilise avec des [clés composites](../attributes.md#composite-key). Il est similaire à [sparse&#95;hashed](#sparse_hashed).

Exemple de configuration :

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(COMPLEX_KEY_SPARSE_HASHED([SHARDS 1] [SHARD_LOAD_QUEUE_BACKLOG 10000] [MAX_LOAD_FACTOR 0.5]))
    ```
  </TabItem>

  <TabItem value="xml" label="Fichier de configuration">
    ```xml
    <layout>
      <complex_key_sparse_hashed>
        <!-- <shards>1</shards> -->
        <!-- <shard_load_queue_backlog>10000</shard_load_queue_backlog> -->
        <!-- <max_load_factor>0.5</max_load_factor> -->
      </complex_key_sparse_hashed>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />