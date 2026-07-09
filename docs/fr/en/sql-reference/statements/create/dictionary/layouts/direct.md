---
slug: /sql-reference/statements/create/dictionary/layouts/direct
title: 'layout de dictionnaire direct'
sidebar_label: 'direct'
sidebar_position: 9
description: 'Un layout de dictionnaire qui interroge la source directement, sans mise en cache.'
doc_type: 'référence'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="direct">
  ## direct
</div>

Le dictionnaire n&#39;est pas stocké en mémoire et interroge directement la source lors du traitement d&#39;une requête.

La clé du dictionnaire est de type [UInt64](/fr/sql-reference/data-types/int-uint.md).

Tous les types de [sources](../sources/#dictionary-sources), à l&#39;exception des fichiers locaux, sont pris en charge.

Exemple de configuration :

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(DIRECT())
    ```
  </TabItem>

  <TabItem value="xml" label="Fichier de configuration">
    ```xml
    <layout>
      <direct />
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

<div id="complex_key_direct">
  ## complex_key_direct
</div>

Ce type de stockage est utilisé avec des [clés](../attributes.md#composite-key) composées. Similaire à `direct`.