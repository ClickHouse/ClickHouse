---
slug: /sql-reference/statements/create/dictionary/layouts/hashed-array
title: 'type de layout de dictionnaire hashed_array'
sidebar_label: 'hashed_array'
sidebar_position: 4
description: 'Stocke un dictionnaire en mémoire à l’aide d’une table de hachage avec des tableaux d’attributs.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="hashed_array">
  ## hashed_array
</div>

Le dictionnaire est entièrement stocké en mémoire. Chaque attribut est stocké dans un tableau. L’attribut clé est stocké sous la forme d’une table de hachage, où la valeur correspond à un index dans le tableau des attributs. Le dictionnaire peut contenir un nombre quelconque d’éléments avec n’importe quels identifiants. En pratique, le nombre de clés peut atteindre plusieurs dizaines de millions.

La clé du dictionnaire est de type [UInt64](/fr/sql-reference/data-types/int-uint.md).

Tous les types de sources sont pris en charge. Lors des mises à jour, les données (provenant d’un fichier ou d’une table) sont lues dans leur intégralité.

Exemple de configuration :

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(HASHED_ARRAY([SHARDS 1]))
    ```
  </TabItem>

  <TabItem value="xml" label="Fichier de configuration">
    ```xml
    <layout>
      <hashed_array>
      </hashed_array>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

<div id="complex_key_hashed_array">
  ## complex_key_hashed_array
</div>

Ce type de stockage s&#39;utilise avec des [clés](../attributes.md#composite-key) composées. Similaire à [hashed&#95;array](#hashed_array).

Exemple de configuration :

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(COMPLEX_KEY_HASHED_ARRAY([SHARDS 1]))
    ```
  </TabItem>

  <TabItem value="xml" label="Fichier de configuration">
    ```xml
    <layout>
      <complex_key_hashed_array />
    </layout>
    ```
  </TabItem>
</Tabs>

<br />