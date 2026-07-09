---
slug: /sql-reference/statements/create/dictionary/layouts/flat
title: 'layout flat de dictionnaire'
sidebar_label: 'flat'
sidebar_position: 2
description: 'Stocke un dictionnaire en mémoire dans des tableaux plats.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Avec la disposition `flat`, le dictionnaire est entièrement stocké en mémoire sous forme de tableaux plats.
La quantité de mémoire utilisée est proportionnelle à la valeur de la clé la plus élevée.

:::tip
Ce type de disposition offre les meilleures performances parmi toutes les méthodes disponibles pour stocker un dictionnaire.
:::

La clé du dictionnaire est de type [UInt64](/fr/sql-reference/data-types/int-uint.md) et sa valeur est limitée à `max_array_size` (par défaut — 500,000).
Si une clé plus grande est détectée lors de la création du dictionnaire, ClickHouse lève une exception et ne crée pas le dictionnaire.
La taille initiale des tableaux plats du dictionnaire est définie par le paramètre `initial_array_size` (par défaut — 1024).

Tous les types de sources sont pris en charge.
Lors de la mise à jour du dictionnaire, les données (qu&#39;elles proviennent d&#39;un fichier ou d&#39;une table) sont lues dans leur intégralité.

Exemple de configuration :

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(FLAT(INITIAL_ARRAY_SIZE 50000 MAX_ARRAY_SIZE 5000000))
    ```
  </TabItem>

  <TabItem value="xml" label="Fichier de configuration">
    ```xml
    <layout>
      <flat>
        <initial_array_size>50000</initial_array_size>
        <max_array_size>5000000</max_array_size>
      </flat>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />