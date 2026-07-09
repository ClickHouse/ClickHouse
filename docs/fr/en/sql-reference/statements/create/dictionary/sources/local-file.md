---
slug: /sql-reference/statements/create/dictionary/sources/local-file
title: 'Source de dictionnaire de type fichier local'
sidebar_position: 2
sidebar_label: 'Fichier local'
description: 'Configurer un fichier local comme source de dictionnaire dans ClickHouse.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

La source de fichier locale charge les données du dictionnaire depuis un fichier sur le système de fichiers local. C’est utile pour les petites tables de correspondance statiques qui peuvent être stockées sous forme de fichiers plats dans des formats tels que TSV, CSV ou tout autre [format pris en charge](/fr/sql-reference/formats).

Exemple de paramètres :

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(FILE(path './user_files/os.tsv' format 'TabSeparated'))
    ```
  </TabItem>

  <TabItem value="xml" label="Fichier de configuration">
    ```xml
    <source>
      <file>
        <path>/opt/dictionaries/os.tsv</path>
        <format>TabSeparated</format>
      </file>
    </source>
    ```
  </TabItem>
</Tabs>

<br />

Champs des paramètres :

| Paramètre | Description                                                                                                |
| --------- | ---------------------------------------------------------------------------------------------------------- |
| `path`    | Le chemin absolu du fichier.                                                                               |
| `format`  | Le format du fichier. Tous les formats décrits dans [Formats](/fr/sql-reference/formats) sont pris en charge. |

Lorsqu’un dictionnaire avec la source `FILE` est créé via une commande DDL (`CREATE DICTIONARY ...`), le fichier source doit se trouver dans le répertoire `user_files` afin d’empêcher les utilisateurs de la base de données d’accéder à des fichiers arbitraires sur le nœud ClickHouse.

**Voir aussi**

* [Fonction dictionary](/fr/sql-reference/table-functions/dictionary)