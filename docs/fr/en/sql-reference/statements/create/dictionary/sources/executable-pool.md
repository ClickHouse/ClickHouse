---
slug: /sql-reference/statements/create/dictionary/sources/executable-pool
title: 'Source de dictionnaire Executable Pool'
sidebar_position: 4
sidebar_label: 'Executable Pool'
description: 'Configurer Executable Pool comme source de dictionnaire dans ClickHouse.'
doc_type: 'référence'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Executable pool permet de charger des données à partir d’un pool de processus.
Cette source ne fonctionne pas avec les layouts de dictionnaire qui doivent charger toutes les données depuis la source.

Executable pool fonctionne si le dictionnaire [est stocké](../layouts/#storing-dictionaries-in-memory) à l’aide de l’un des layouts suivants :

* `cache`
* `complex_key_cache`
* `ssd_cache`
* `complex_key_ssd_cache`
* `direct`
* `complex_key_direct`

Executable pool lance un pool de processus avec la commande spécifiée et les maintient en cours d’exécution jusqu’à ce qu’ils se terminent. Le programme doit lire les données depuis STDIN tant qu’elles sont disponibles et écrire le résultat sur STDOUT. Il peut attendre le bloc de données suivant sur STDIN. ClickHouse ne fermera pas STDIN après le traitement d’un bloc de données, mais y enverra un autre chunk de données lorsque nécessaire. Le script exécutable doit être conçu pour ce mode de traitement des données : il doit interroger STDIN et vider les données vers STDOUT sans attendre.

Exemple de paramètres :

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(EXECUTABLE_POOL(
        command 'while read key; do printf "$key\tData for key $key\n"; done'
        format 'TabSeparated'
        pool_size 10
        max_command_execution_time 10
        implicit_key false
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="Fichier de configuration">
    ```xml
    <source>
        <executable_pool>
            <command><command>while read key; do printf "$key\tData for key $key\n"; done</command</command>
            <format>TabSeparated</format>
            <pool_size>10</pool_size>
            <max_command_execution_time>10<max_command_execution_time>
            <implicit_key>false</implicit_key>
        </executable_pool>
    </source>
    ```
  </TabItem>
</Tabs>

Champs de paramétrage :

| Setting                       | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| ----------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `command`                     | Le chemin absolu vers le fichier exécutable, ou le nom du fichier (si le répertoire du programme est indiqué dans `PATH`).                                                                                                                                                                                                                                                                                                                                             |
| `format`                      | Le format de fichier. Tous les formats décrits dans [Formats](/fr/sql-reference/formats) sont pris en charge.                                                                                                                                                                                                                                                                                                                                                             |
| `pool_size`                   | Taille du pool. Si `0` est spécifié pour `pool_size`, alors la taille du pool n’est soumise à aucune restriction. La valeur par défaut est `16`.                                                                                                                                                                                                                                                                                                                       |
| `command_termination_timeout` | Le script exécutable doit contenir une boucle principale de lecture-écriture. Une fois le dictionnaire détruit, le pipe est fermé, et le fichier exécutable dispose de `command_termination_timeout` secondes pour s’arrêter avant que ClickHouse n’envoie le signal SIGTERM au processus enfant. Exprimé en secondes. La valeur par défaut est `10`. Facultatif.                                                                                                      |
| `max_command_execution_time`  | Temps d’exécution maximal de la commande du script exécutable pour traiter un bloc de données. Exprimé en secondes. La valeur par défaut est `10`. Facultatif.                                                                                                                                                                                                                                                                                                         |
| `command_read_timeout`        | Délai d’attente pour la lecture des données depuis stdout de la commande, en millisecondes. Valeur par défaut : `10000`. Facultatif.                                                                                                                                                                                                                                                                                                                                   |
| `command_write_timeout`       | Délai d’attente pour l’écriture des données dans stdin de la commande, en millisecondes. Valeur par défaut : `10000`. Facultatif.                                                                                                                                                                                                                                                                                                                                      |
| `implicit_key`                | Le fichier source exécutable peut ne renvoyer que les valeurs, et la correspondance avec les clés demandées est alors déterminée implicitement par l’ordre des lignes dans le résultat. La valeur par défaut est `false`. Facultatif.                                                                                                                                                                                                                                  |
| `execute_direct`              | Si `execute_direct` = `1`, alors `command` sera recherché dans le dossier user&#95;scripts spécifié par [user&#95;scripts&#95;path](/fr/operations/server-configuration-parameters/settings#user_scripts_path). Des arguments de script supplémentaires peuvent être spécifiés en les séparant par des espaces. Exemple : `script_name arg1 arg2`. Si `execute_direct` = `0`, `command` est passé comme argument à `bin/sh -c`. La valeur par défaut est `1`. Facultatif. |
| `send_chunk_header`           | Contrôle s’il faut envoyer le nombre de lignes avant d’envoyer un chunk de données au processus. La valeur par défaut est `false`. Facultatif.                                                                                                                                                                                                                                                                                                                         |

Cette source de dictionnaire ne peut être configurée que via une configuration XML. La création de dictionnaires avec une source exécutable via DDL est désactivée, faute de quoi l’utilisateur de la base de données pourrait exécuter un binaire arbitraire sur le nœud ClickHouse.