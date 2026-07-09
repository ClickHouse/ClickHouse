---
slug: /sql-reference/statements/create/dictionary/layouts/ssd-cache
title: 'types de layout de dictionnaire ssd_cache'
sidebar_label: 'ssd_cache'
sidebar_position: 8
description: 'Stocke les données du dictionnaire sur SSD avec un index en mémoire : types ssd_cache ou complex_key_ssd_cache'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="ssd_cache">
  ## ssd_cache
</div>

Semblable à `cache`, mais stocke les données sur SSD et l’index en RAM. Tous les paramètres des dictionnaires `cache` liés à la file d’attente de mise à jour peuvent également s’appliquer aux dictionnaires `ssd_cache`.

La clé du dictionnaire est de type [UInt64](/fr/sql-reference/data-types/int-uint.md).

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(SSD_CACHE(BLOCK_SIZE 4096 FILE_SIZE 16777216 READ_BUFFER_SIZE 1048576
        PATH '/var/lib/clickhouse/user_files/test_dict'))
    ```
  </TabItem>

  <TabItem value="xml" label="Fichier de configuration">
    ```xml
    <layout>
        <ssd_cache>
            <!-- Taille du bloc de lecture élémentaire en octets. Il est recommandé qu’elle soit égale à la taille de page du SSD. -->
            <block_size>4096</block_size>
            <!-- Taille maximale du fichier de cache en octets. -->
            <file_size>16777216</file_size>
            <!-- Taille du tampon RAM en octets pour lire les éléments depuis le SSD. -->
            <read_buffer_size>131072</read_buffer_size>
            <!-- Taille du tampon RAM en octets pour agréger les éléments avant leur écriture sur le SSD. -->
            <write_buffer_size>1048576</write_buffer_size>
            <!-- Chemin où sera stocké le fichier de cache. -->
            <path>/var/lib/clickhouse/user_files/test_dict</path>
        </ssd_cache>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

<div id="complex_key_ssd_cache">
  ## complex_key_ssd_cache
</div>

Ce type de stockage s’utilise avec des [clés composites](../attributes.md#composite-key). Semblable à `ssd_cache`.