---
slug: /sql-reference/statements/create/dictionary/sources/executable-file
title: 'Source de dictionnaire « Fichier exécutable »'
sidebar_position: 3
sidebar_label: 'Fichier exécutable'
description: 'Configurer un fichier exécutable comme source de dictionnaire dans ClickHouse.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

L’utilisation de fichiers exécutables dépend de [la manière dont le dictionnaire est stocké en mémoire](../layouts/). Si le dictionnaire est stocké avec `cache` et `complex_key_cache`, ClickHouse demande les clés nécessaires en envoyant une requête à l’entrée standard (STDIN) du fichier exécutable. Sinon, ClickHouse lance le fichier exécutable et considère sa sortie comme les données du dictionnaire.

Exemple de paramètres :

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(EXECUTABLE(
        command 'cat /opt/dictionaries/os.tsv'
        format 'TabSeparated'
        implicit_key false
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="Fichier de configuration">
    ```xml
    <source>
        <executable>
            <command>cat /opt/dictionaries/os.tsv</command>
            <format>TabSeparated</format>
            <implicit_key>false</implicit_key>
        </executable>
    </source>
    ```
  </TabItem>
</Tabs>

Champs des paramètres :

| Paramètre                     | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| ----------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `command`                     | Le chemin absolu vers le fichier exécutable, ou le nom du fichier (si le répertoire de la commande figure dans le `PATH`).                                                                                                                                                                                                                                                                                                                                        |
| `format`                      | Le format du fichier. Tous les formats décrits dans [Formats](/fr/sql-reference/formats) sont pris en charge.                                                                                                                                                                                                                                                                                                                                                        |
| `command_termination_timeout` | Le script exécutable doit contenir une boucle principale de lecture-écriture. Une fois le dictionnaire détruit, le pipe est fermé, et le fichier exécutable dispose de `command_termination_timeout` secondes pour s’arrêter avant que ClickHouse n’envoie un signal SIGTERM au processus enfant. Exprimé en secondes. La valeur par défaut est `10`. Facultatif.                                                                                                 |
| `command_read_timeout`        | Délai d’attente pour la lecture des données depuis la sortie standard (stdout) de la commande, en millisecondes. La valeur par défaut est `10000`. Facultatif.                                                                                                                                                                                                                                                                                                    |
| `command_write_timeout`       | Délai d’attente pour l’écriture des données vers l’entrée standard (stdin) de la commande, en millisecondes. La valeur par défaut est `10000`. Facultatif.                                                                                                                                                                                                                                                                                                        |
| `implicit_key`                | Le fichier source exécutable peut ne renvoyer que les valeurs, et la correspondance avec les clés demandées est alors déterminée implicitement par l’ordre des lignes dans le résultat. La valeur par défaut est `false`.                                                                                                                                                                                                                                         |
| `execute_direct`              | Si `execute_direct` = `1`, `command` est recherché dans le dossier user&#95;scripts spécifié par [user&#95;scripts&#95;path](/fr/operations/server-configuration-parameters/settings#user_scripts_path). Des arguments de script supplémentaires peuvent être indiqués en les séparant par des espaces. Exemple : `script_name arg1 arg2`. Si `execute_direct` = `0`, `command` est transmis comme argument à `bin/sh -c`. La valeur par défaut est `0`. Facultatif. |
| `send_chunk_header`           | Indique s’il faut envoyer le nombre de lignes avant d’envoyer un chunk de données au processus. La valeur par défaut est `false`. Facultatif.                                                                                                                                                                                                                                                                                                                     |

Cette source de dictionnaire ne peut être configurée que via une configuration XML. La création de dictionnaires avec une source exécutable via DDL est désactivée ; sinon, l’utilisateur de la base de données pourrait exécuter des binaires arbitraires sur le nœud ClickHouse.