---
description: 'Documentation des fonctions définies par l’utilisateur (UDFs)'
sidebar_label: 'UDF'
slug: /sql-reference/functions/udf
title: 'Fonctions définies par l’utilisateur (UDFs)'
doc_type: 'reference'
---

import BetaBadge from '@theme/badges/BetaBadge';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

<div id="udfs-user-defined-functions">
  # UDFs Fonctions définies par l’utilisateur
</div>

ClickHouse prend en charge plusieurs types de fonctions définies par l’utilisateur (UDFs) :

* Les [Executable UDFs](#executable-user-defined-functions) lancent un programme ou un script externe (Python, Bash, etc.) et lui transmettent des blocs de données en flux via STDIN / STDOUT. Utilisez-les pour intégrer du code ou des outils existants sans recompiler ClickHouse. Elles impliquent une surcharge plus élevée par appel que les options en cours de processus et conviennent surtout à une logique plus lourde ou lorsqu’un environnement d’exécution différent est nécessaire.
* Les [SQL UDFs](#sql-user-defined-functions) sont définies avec `CREATE FUNCTION` uniquement en SQL. Elles sont intégrées au plan de requête (sans séparation de processus), ce qui les rend légères et idéales pour réutiliser une logique d’expression ou simplifier des colonnes calculées complexes.
* Les [WebAssembly UDFs expérimentales](#webassembly-user-defined-functions) exécutent du code compilé en WebAssembly dans un environnement isolé au sein du processus serveur. Elles offrent une surcharge par appel plus faible que les exécutables externes, avec une meilleure isolation que les extensions natives, ce qui les rend adaptées aux algorithmes personnalisés écrits dans des langages pouvant cibler WASM (par ex. C/C++/Rust).
* Les [driver-based executable UDFs expérimentales](#driver-based-executable-user-defined-functions) permettent à un « driver » fourni par l’opérateur de transformer un extrait de code fourni dans `CREATE FUNCTION ... ENGINE = DriverName(...) AS '...'` en executable UDF au moment de la création de la fonction (par exemple, en le compilant). Elles s’appuient sur les executable UDFs et nécessitent une configuration du driver côté serveur.

<div id="executable-user-defined-functions">
  ## Fonctions exécutables définies par l’utilisateur
</div>

<BetaBadge />

:::note
Dans ClickHouse Cloud, les UDF exécutables sont en bêta publique et sont créées via l’interface utilisateur de la Cloud Console. Voir [User-defined functions in Cloud](/fr/cloud/features/user-defined-functions) pour la procédure spécifique à Cloud.
:::

ClickHouse peut appeler n’importe quel programme exécutable externe ou script pour traiter les données.

La configuration des fonctions exécutables définies par l’utilisateur peut se trouver dans un ou plusieurs fichiers XML.
Le chemin d’accès à la configuration est spécifié dans le paramètre [`user_defined_executable_functions_config`](../../operations/server-configuration-parameters/settings.md#user_defined_executable_functions_config).

Une configuration de fonction contient les paramètres suivants :

| Paramètre                     | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                              | Obligatoire | Valeur par défaut         |
| ----------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------- | ------------------------- |
| `name`                        | Nom d’une fonction                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | Oui         | -                         |
| `command`                     | Nom du script à exécuter, ou commande si `execute_direct` est défini sur false                                                                                                                                                                                                                                                                                                                                                                                                           | Oui         | -                         |
| `argument`                    | Description de l’argument avec son `type` et, éventuellement, son `name`. Chaque argument est décrit dans un paramètre distinct. Indiquer un nom est nécessaire si les noms d’arguments font partie de la sérialisation pour un format de fonction définie par l’utilisateur tel que [Native](/fr/interfaces/formats/Native) ou [JSONEachRow](/fr/interfaces/formats/JSONEachRow)                                                                                                              | Oui         | `c` + argument&#95;number |
| `format`                      | [Format](../../interfaces/formats.md) dans lequel les arguments sont transmis à la commande. La sortie de la commande doit également utiliser ce même format                                                                                                                                                                                                                                                                                                                             | Oui         | -                         |
| `return_type`                 | Type de la valeur renvoyée                                                                                                                                                                                                                                                                                                                                                                                                                                                               | Oui         | -                         |
| `return_name`                 | Nom de la valeur renvoyée. Indiquer un nom de retour est nécessaire si ce nom fait partie de la sérialisation pour un format de fonction définie par l’utilisateur tel que [Native](/fr/interfaces/formats/Native) ou [JSONEachRow](/fr/interfaces/formats/JSONEachRow)                                                                                                                                                                                                                        | Facultatif  | `result`                  |
| `type`                        | Type d’exécutable. Si `type` est défini sur `executable`, une seule commande est démarrée. S’il est défini sur `executable_pool`, un pool de commandes est créé                                                                                                                                                                                                                                                                                                                          | Oui         | -                         |
| `max_command_execution_time`  | Temps d’exécution maximal, en secondes, pour traiter un bloc de données. Ce paramètre n’est valable que pour les commandes `executable_pool`                                                                                                                                                                                                                                                                                                                                             | Facultatif  | `10`                      |
| `command_termination_timeout` | Durée, en secondes, accordée à une commande pour se terminer après la fermeture de son pipe. Passé ce délai, `SIGTERM` est envoyé au processus exécutant la commande                                                                                                                                                                                                                                                                                                                     | Facultatif  | `10`                      |
| `command_read_timeout`        | Délai d’attente pour la lecture des données depuis le stdout de la commande, en millisecondes                                                                                                                                                                                                                                                                                                                                                                                            | Facultatif  | `10000`                   |
| `command_write_timeout`       | Délai d’attente pour l’écriture des données vers le stdin de la commande, en millisecondes                                                                                                                                                                                                                                                                                                                                                                                               | Facultatif  | `10000`                   |
| `pool_size`                   | Taille d’un pool de commandes                                                                                                                                                                                                                                                                                                                                                                                                                                                            | Facultatif  | `16`                      |
| `send_chunk_header`           | Contrôle l’envoi du nombre de lignes avant l’envoi d’un fragment de données au processus                                                                                                                                                                                                                                                                                                                                                                                                 | Facultatif  | `false`                   |
| `execute_direct`              | Si `execute_direct` = `1`, `command` est recherché dans le dossier user&#95;scripts spécifié par [user&#95;scripts&#95;path](../../operations/server-configuration-parameters/settings.md#user_scripts_path). Des arguments de script supplémentaires peuvent être indiqués en les séparant par des espaces. Exemple : `script_name arg1 arg2`. Si `execute_direct` = `0`, `command` est transmis comme argument à `bin/sh -c`                                                           | Facultatif  | `1`                       |
| `lifetime`                    | Intervalle de rechargement d’une fonction, en secondes. S’il est défini sur `0`, la fonction n’est pas rechargée                                                                                                                                                                                                                                                                                                                                                                         | Facultatif  | `0`                       |
| `deterministic`               | Indique si la fonction est déterministe (renvoie le même résultat pour la même entrée)                                                                                                                                                                                                                                                                                                                                                                                                   | Facultatif  | `false`                   |
| `stderr_reaction`             | Indique comment gérer la sortie stderr de la commande. Valeurs : `none` (ignorer), `log` (journaliser immédiatement toute la sortie stderr), `log_first` (journaliser les 4 premiers KiB après la fin), `log_last` (journaliser les 4 derniers KiB après la fin), `throw` (lever immédiatement une exception à la moindre sortie stderr). Lors de l’utilisation de `log_first` ou `log_last` avec un code de sortie non nul, le contenu de stderr est inclus dans le message d’exception | Facultatif  | `log_last`                |
| `check_exit_code`             | Si true, ClickHouse vérifie le code de sortie de la commande. Un code de sortie non nul provoque une exception                                                                                                                                                                                                                                                                                                                                                                           | Facultatif  | `true`                    |

La commande doit lire les arguments depuis `STDIN` et écrire le résultat vers `STDOUT`. Elle doit traiter les arguments de manière itérative. Autrement dit, après avoir traité un fragment d’arguments, elle doit attendre le fragment suivant.

<div id="executable-user-defined-functions">
  ## Fonctions exécutables définies par l’utilisateur
</div>

<div id="examples">
  ## Exemples
</div>

<div id="udf-inline">
  ### UDF à partir d&#39;un script intégré
</div>

Créez `test_function_sum` en définissant manuellement `execute_direct` sur `0` à l&#39;aide d&#39;une configuration XML ou YAML.

<Tabs>
  <TabItem value="XML" label="XML" default>
    Fichier `test_function.xml` (`/etc/clickhouse-server/test_function.xml` avec la configuration de chemin par défaut).

    ```xml title="/etc/clickhouse-server/test_function.xml"
    <functions>
        <function>
            <type>executable</type>
            <name>test_function_sum</name>
            <return_type>UInt64</return_type>
            <argument>
                <type>UInt64</type>
                <name>lhs</name>
            </argument>
            <argument>
                <type>UInt64</type>
                <name>rhs</name>
            </argument>
            <format>TabSeparated</format>
            <command>cd /; clickhouse-local --input-format TabSeparated --output-format TabSeparated --structure 'x UInt64, y UInt64' --query "SELECT x + y FROM table"</command>
            <execute_direct>0</execute_direct>
            <deterministic>true</deterministic>
        </function>
    </functions>
    ```
  </TabItem>

  <TabItem value="YAML" label="YAML">
    Fichier `test_function.yaml` (`/etc/clickhouse-server/test_function.yaml` avec la configuration de chemin par défaut).

    ```yml title="/etc/clickhouse-server/test_function.yaml"
    functions:
      type: executable
      name: test_function_sum
      return_type: UInt64
      argument:
        - type: UInt64
          name: lhs
        - type: UInt64
          name: rhs
      format: TabSeparated
      command: 'cd /; clickhouse-local --input-format TabSeparated --output-format TabSeparated --structure ''x UInt64, y UInt64'' --query "SELECT x + y FROM table"'
      execute_direct: 0
      deterministic: true
    ```
  </TabItem>
</Tabs>

<br />

```sql title="Query"
SELECT test_function_sum(2, 2);
```

```text title="Result"
┌─test_function_sum(2, 2)─┐
│                       4 │
└─────────────────────────┘
```

<div id="udf-python">
  ### UDF à partir d’un script Python
</div>

Dans cet exemple, nous créons une UDF qui lit une valeur sur `STDIN` et la renvoie sous forme de chaîne.

Créez `test_function` à l’aide d’une configuration XML ou YAML.

<Tabs>
  <TabItem value="XML" label="XML" default>
    Fichier `test_function.xml` (`/etc/clickhouse-server/test_function.xml` avec le chemin par défaut).

    ```xml title="/etc/clickhouse-server/test_function.xml"
    <functions>
        <function>
            <type>executable</type>
            <name>test_function_python</name>
            <return_type>String</return_type>
            <argument>
                <type>UInt64</type>
                <name>value</name>
            </argument>
            <format>TabSeparated</format>
            <command>test_function.py</command>
        </function>
    </functions>
    ```
  </TabItem>

  <TabItem value="YAML" label="YAML">
    Fichier `test_function.yaml` (`/etc/clickhouse-server/test_function.yaml` avec le chemin par défaut).

    ```yml title="/etc/clickhouse-server/test_function.yaml"
    functions:
      type: executable
      name: test_function_python
      return_type: String
      argument:
        - type: UInt64
          name: value
      format: TabSeparated
      command: test_function.py
    ```
  </TabItem>
</Tabs>

<br />

Créez le fichier de script `test_function.py` dans le dossier `user_scripts` (`/var/lib/clickhouse/user_scripts/test_function.py` avec le chemin par défaut).

```python
#!/usr/bin/python3

import sys

if __name__ == '__main__':
    for line in sys.stdin:
        print("Value " + line, end='')
        sys.stdout.flush()
```

```sql title="Query"
SELECT test_function_python(toUInt64(2));
```

```text title="Result"
┌─test_function_python(2)─┐
│ Value 2                 │
└─────────────────────────┘
```

<div id="udf-stdin">
  ### Lire deux valeurs à partir de `STDIN` et renvoyer leur somme sous la forme d’un objet JSON
</div>

Créez `test_function_sum_json` avec des arguments nommés et le format [JSONEachRow](/fr/interfaces/formats/JSONEachRow) à l’aide d’une configuration XML ou YAML.

<Tabs>
  <TabItem value="XML" label="XML" default>
    Fichier `test_function.xml` (`/etc/clickhouse-server/test_function.xml` avec les chemins par défaut).

    ```xml title="/etc/clickhouse-server/test_function.xml"
    <functions>
        <function>
            <type>executable</type>
            <name>test_function_sum_json</name>
            <return_type>UInt64</return_type>
            <return_name>result_name</return_name>
            <argument>
                <type>UInt64</type>
                <name>argument_1</name>
            </argument>
            <argument>
                <type>UInt64</type>
                <name>argument_2</name>
            </argument>
            <format>JSONEachRow</format>
            <command>test_function_sum_json.py</command>
        </function>
    </functions>
    ```
  </TabItem>

  <TabItem value="YAML" label="YAML">
    Fichier `test_function.yaml` (`/etc/clickhouse-server/test_function.yaml` avec les chemins par défaut).

    ```yml title="/etc/clickhouse-server/test_function.yaml"
    functions:
      type: executable
      name: test_function_sum_json
      return_type: UInt64
      return_name: result_name
      argument:
        - type: UInt64
          name: argument_1
        - type: UInt64
          name: argument_2
      format: JSONEachRow
      command: test_function_sum_json.py
    ```
  </TabItem>
</Tabs>

<br />

Créez le fichier de script `test_function_sum_json.py` dans le dossier `user_scripts` (`/var/lib/clickhouse/user_scripts/test_function_sum_json.py` avec les chemins par défaut).

```python
#!/usr/bin/python3

import sys
import json

if __name__ == '__main__':
    for line in sys.stdin:
        value = json.loads(line)
        first_arg = int(value['argument_1'])
        second_arg = int(value['argument_2'])
        result = {'result_name': first_arg + second_arg}
        print(json.dumps(result), end='\n')
        sys.stdout.flush()
```

```sql title="Query"
SELECT test_function_sum_json(2, 2);
```

```text title="Result"
┌─test_function_sum_json(2, 2)─┐
│                            4 │
└──────────────────────────────┘
```

<div id="udf-parameters-in-command">
  ### Utiliser des paramètres dans le paramètre `command`
</div>

Les fonctions définies par l&#39;utilisateur exécutables peuvent accepter des paramètres constants configurés dans le paramètre `command` (cela fonctionne uniquement pour les fonctions définies par l&#39;utilisateur de type `executable`).
Cela nécessite également l&#39;option `execute_direct` afin d&#39;éviter toute vulnérabilité liée à l&#39;expansion des arguments du shell.

<Tabs>
  <TabItem value="XML" label="XML" default>
    Fichier `test_function_parameter_python.xml` (`/etc/clickhouse-server/test_function_parameter_python.xml` avec la configuration de chemin par défaut).

    ```xml title="/etc/clickhouse-server/test_function_parameter_python.xml"
    <functions>
        <function>
            <type>executable</type>
            <execute_direct>true</execute_direct>
            <name>test_function_parameter_python</name>
            <return_type>String</return_type>
            <argument>
                <type>UInt64</type>
            </argument>
            <format>TabSeparated</format>
            <command>test_function_parameter_python.py {test_parameter:UInt64}</command>
        </function>
    </functions>
    ```
  </TabItem>

  <TabItem value="YAML" label="YAML">
    Fichier `test_function_parameter_python.yaml` (`/etc/clickhouse-server/test_function_parameter_python.yaml` avec la configuration de chemin par défaut).

    ```yml title="/etc/clickhouse-server/test_function_parameter_python.yaml"
    functions:
      type: executable
      execute_direct: true
      name: test_function_parameter_python
      return_type: String
      argument:
        - type: UInt64
      format: TabSeparated
      command: test_function_parameter_python.py {test_parameter:UInt64}
    ```
  </TabItem>
</Tabs>

<br />

Créez le fichier script `test_function_parameter_python.py` dans le dossier `user_scripts` (`/var/lib/clickhouse/user_scripts/test_function_parameter_python.py` avec la configuration de chemin par défaut).

```python
#!/usr/bin/python3

import sys

if __name__ == "__main__":
    for line in sys.stdin:
        print("Parameter " + str(sys.argv[1]) + " value " + str(line), end="")
        sys.stdout.flush()
```

```sql title="Query"
SELECT test_function_parameter_python(1)(2);
```

```text title="Result"
┌─test_function_parameter_python(1)(2)─┐
│ Parameter 1 value 2                  │
└──────────────────────────────────────┘
```

<div id="udf-shell-script">
  ### UDF à partir d’un script shell
</div>

Dans cet exemple, nous créons un script shell qui multiplie chaque valeur par 2.

<Tabs>
  <TabItem value="XML" label="XML" default>
    Fichier `test_function_shell.xml` (`/etc/clickhouse-server/test_function_shell.xml` avec le chemin par défaut).

    ```xml title="/etc/clickhouse-server/test_function_shell.xml"
    <functions>
        <function>
            <type>executable</type>
            <name>test_shell</name>
            <return_type>String</return_type>
            <argument>
                <type>UInt8</type>
                <name>value</name>
            </argument>
            <format>TabSeparated</format>
            <command>test_shell.sh</command>
        </function>
    </functions>
    ```
  </TabItem>

  <TabItem value="YAML" label="YAML">
    Fichier `test_function_shell.yaml` (`/etc/clickhouse-server/test_function_shell.yaml` avec le chemin par défaut).

    ```yml title="/etc/clickhouse-server/test_function_shell.yaml"
    functions:
      type: executable
      name: test_shell
      return_type: String
      argument:
        - type: UInt8
          name: value
      format: TabSeparated
      command: test_shell.sh
    ```
  </TabItem>
</Tabs>

<br />

Créez le fichier de script `test_shell.sh` dans le dossier `user_scripts` (`/var/lib/clickhouse/user_scripts/test_shell.sh` avec le chemin par défaut).

```bash title="/var/lib/clickhouse/user_scripts/test_shell.sh"
#!/bin/bash

while read read_data;
    do printf "$(expr $read_data \* 2)\n";
done
```

```sql title="Query"
SELECT test_shell(number) FROM numbers(10);
```

```text title="Result"
    ┌─test_shell(number)─┐
 1. │ 0                  │
 2. │ 2                  │
 3. │ 4                  │
 4. │ 6                  │
 5. │ 8                  │
 6. │ 10                 │
 7. │ 12                 │
 8. │ 14                 │
 9. │ 16                 │
10. │ 18                 │
    └────────────────────┘
```

<div id="error-handling">
  ## Gestion des erreurs
</div>

Certaines fonctions peuvent lever une exception si les données ne sont pas valides.
Dans ce cas, la requête est annulée et un message d’erreur est renvoyé au client.
Dans le cadre du traitement distribué, lorsqu’une exception se produit sur l’un des serveurs, les autres serveurs tentent également d’interrompre la requête.

<div id="evaluation-of-argument-expressions">
  ## Évaluation des expressions d’arguments
</div>

Dans presque tous les langages de programmation, pour certains opérateurs, il arrive qu’un des arguments ne soit pas évalué.
Il s’agit généralement des opérateurs `&&`, `||` et `?:`.
Dans ClickHouse, les arguments des fonctions (opérateurs) sont toujours évalués.
Cela s’explique par le fait que des blocs entiers de colonnes sont évalués d’un seul coup, au lieu de calculer chaque ligne séparément.

<div id="performing-functions-for-distributed-query-processing">
  ## Exécution de fonctions pour le traitement distribué des requêtes
</div>

Pour le traitement distribué des requêtes, le plus grand nombre possible d&#39;étapes du traitement de la requête sont exécutées sur des serveurs distants, et les étapes restantes (la fusion des résultats intermédiaires et tout ce qui suit) sont exécutées sur le serveur demandeur.

Cela signifie que les fonctions peuvent être exécutées sur différents serveurs.
Par exemple, dans la requête `SELECT f(sum(g(x))) FROM distributed_table GROUP BY h(y),`

* si `distributed_table` comporte au moins deux shards, les fonctions &#39;g&#39; et &#39;h&#39; sont exécutées sur des serveurs distants, et la fonction &#39;f&#39; est exécutée sur le serveur demandeur.
* si `distributed_table` ne comporte qu&#39;un seul shard, toutes les fonctions &#39;f&#39;, &#39;g&#39; et &#39;h&#39; sont exécutées sur le serveur de ce shard.

Le résultat d&#39;une fonction ne dépend généralement pas du serveur sur lequel elle est exécutée. Cependant, cela peut parfois avoir de l&#39;importance.
Par exemple, les fonctions qui utilisent des dictionnaires s&#39;appuient sur le dictionnaire présent sur le serveur où elles s&#39;exécutent.
Autre exemple : la fonction `hostName`, qui renvoie le nom du serveur sur lequel elle s&#39;exécute afin de permettre un `GROUP BY` par serveur dans une requête `SELECT`.

Si une fonction d&#39;une requête est exécutée sur le serveur demandeur, mais que vous devez l&#39;exécuter sur des serveurs distants, vous pouvez l&#39;encapsuler dans une fonction d&#39;agrégation &#39;any&#39; ou l&#39;ajouter comme clé dans `GROUP BY`.

<div id="sql-user-defined-functions">
  ## Fonctions définies par l’utilisateur en SQL
</div>

Des fonctions personnalisées basées sur des expressions lambda peuvent être créées à l’aide de l’instruction [CREATE FUNCTION](../statements/create/function.md). Pour supprimer ces fonctions, utilisez l’instruction [DROP FUNCTION](../statements/drop.md#drop-function).

<div id="webassembly-user-defined-functions">
  ## Fonctions WebAssembly définies par l&#39;utilisateur
</div>

<CloudNotSupportedBadge />

<ExperimentalBadge />

Les fonctions WebAssembly définies par l&#39;utilisateur (WASM UDFs) vous permettent d&#39;exécuter du code personnalisé compilé en WebAssembly dans le processus du serveur ClickHouse.

<div id="quick-start">
  ### Démarrage rapide
</div>

Activez la prise en charge expérimentale de WebAssembly dans la configuration de ClickHouse :

```xml
<clickhouse>
    <allow_experimental_webassembly_udf>true</allow_experimental_webassembly_udf>
</clickhouse>
```

Insérez votre module WASM compilé dans la table système :

```sql
INSERT INTO system.webassembly_modules (name, code)
SELECT 'my_module', base64Decode('AGFzbQEAAAA...');
```

Créez une fonction à l’aide de votre module WASM :

```sql
CREATE FUNCTION my_function
LANGUAGE WASM
ABI ROW_DIRECT
FROM 'my_module'
ARGUMENTS (x UInt32, y UInt32)
RETURNS UInt32;
```

Utilisez la fonction dans vos requêtes :

```sql
SELECT my_function(10, 20);
```

<div id="more-information">
  ### Informations complémentaires
</div>

Consultez la documentation sur les [Fonction WebAssembly définie par l’utilisateur](wasm_udf.md) pour en savoir plus.

<div id="driver-based-executable-user-defined-functions">
  ## Fonctions utilisateur exécutables basées sur un driver
</div>

<CloudNotSupportedBadge />

<ExperimentalBadge />

:::note
Il s’agit d’une fonctionnalité expérimentale qui pourra évoluer de façon non rétrocompatible dans les versions futures. Activez-la avec le paramètre de serveur [`allow_experimental_executable_udf_drivers`](../../operations/server-configuration-parameters/settings.md#allow_experimental_executable_udf_drivers).
:::

Un *driver* est un adaptateur fourni par l’opérateur qui transforme un extrait de code utilisateur en [UDF exécutable](#executable-user-defined-functions). Lorsqu’une fonction est créée avec `ENGINE = DriverName(...)`, ClickHouse exécute la commande `create_command` du driver en lui transmettant la signature de la fonction et le code source ; le driver compile ce code ou le traite autrement, puis produit une configuration d’UDF exécutable que ClickHouse stocke et charge ensuite.

Cela permet aux administrateurs d’offrir aux utilisateurs un moyen sûr et limité de définir des fonctions dans un langage quelconque (par exemple, du C compilé dans un conteneur isolé) sans leur donner accès aux fichiers de configuration ni au système de fichiers du serveur. L’ensemble des drivers disponibles est entièrement contrôlé par l’opérateur.

<div id="enabling-drivers">
  ### Activation des drivers
</div>

Les driver-based executable UDFs sont désactivées par défaut. Pour les activer :

1. Définissez l&#39;option Experimental dans la configuration du serveur :

   ```xml
   <clickhouse>
       <allow_experimental_executable_udf_drivers>true</allow_experimental_executable_udf_drivers>
   </clickhouse>
   ```

2. Faites pointer [`user_defined_executable_function_drivers_config`](../../operations/server-configuration-parameters/settings.md#user_defined_executable_function_drivers_config) vers un ou plusieurs fichiers de configuration de driver (les globs sont pris en charge) et, si nécessaire, définissez [`dynamic_user_defined_executable_functions_path`](../../operations/server-configuration-parameters/settings.md#dynamic_user_defined_executable_functions_path), le répertoire dans lequel sont stockées les configurations d&#39;executable UDF générées :

   ```xml
   <clickhouse>
       <user_defined_executable_function_drivers_config>user_defined_executable_function_drivers_config.d/*_driver.xml</user_defined_executable_function_drivers_config>
       <dynamic_user_defined_executable_functions_path>/var/lib/clickhouse/dynamic_user_defined_executable_functions/</dynamic_user_defined_executable_functions_path>
   </clickhouse>
   ```

Le registre des drivers est chargé au démarrage du serveur et rechargé lors de `SYSTEM RELOAD CONFIG`, de sorte que des drivers peuvent être ajoutés, modifiés ou supprimés sans redémarrer le serveur.

<div id="driver-configuration">
  ### Configuration du driver
</div>

Un driver est décrit par un fichier XML (ou YAML) contenant un élément `<driver>` à la racine. Les champs suivants sont pris en charge :

| Champ              | Description                                                                                                                                                                           | Obligatoire |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------- |
| `name`             | Le nom du driver, tel qu&#39;il est utilisé dans `CREATE FUNCTION ... ENGINE = <name>(...)`.                                                                                          | Oui         |
| `create_command`   | Chemin vers le programme appelé pour créer une UDF à partir d&#39;un extrait de code. Les chemins relatifs sont résolus par rapport au fichier de configuration du driver.            | Oui         |
| `drop_command`     | Chemin vers le programme appelé lorsqu&#39;une fonction basée sur ce driver est supprimée.                                                                                            | Non         |
| `engine_arguments` | Déclare les arguments autorisés dans `ENGINE = DriverName(...)`. Chaque élément enfant est un nom d&#39;argument ; un enfant `<required>true</required>` le marque comme obligatoire. | Non         |
| `env`              | Variables d&#39;environnement exportées lors de l&#39;appel des commandes du driver.                                                                                                  | Non         |

Exemple de configuration du driver :

```xml
<clickhouse>
    <driver>
        <name>DockerC</name>
        <create_command>../user_defined_executable_function_drivers/docker_c_create.sh</create_command>
        <drop_command>../user_defined_executable_function_drivers/docker_c_drop.sh</drop_command>
        <engine_arguments>
            <opt_level><required>false</required></opt_level>
        </engine_arguments>
        <env>
            <CLICKHOUSE_C_DRIVER_MEMORY>256m</CLICKHOUSE_C_DRIVER_MEMORY>
            <CLICKHOUSE_C_DRIVER_CPUS>1.0</CLICKHOUSE_C_DRIVER_CPUS>
        </env>
    </driver>
</clickhouse>
```

<div id="driver-invocation-contract">
  #### Contrat d&#39;invocation du driver
</div>

Lorsque `CREATE FUNCTION` s&#39;exécute, `create_command` est invoquée avec les variables `env` configurées et les arguments suivants :

* `--name <function_name>`
* `--return <return_type>` (si une clause `RETURNS` est présente)
* `--args <signature>` (si une clause `ARGUMENTS` est présente), où la signature correspond à la liste des arguments déclarés, par exemple `x UInt8, y DateTime`
* `--<key> <value>` pour chaque argument d&#39;engine déclaré fourni dans `ENGINE = DriverName(key = value)`

Le corps du code utilisateur (le texte après `AS`) est envoyé sur l&#39;entrée standard de la commande. La commande doit écrire la configuration d&#39;une UDF exécutable sur sa sortie standard. Le format est détecté automatiquement : une sortie qui commence par `<` est traitée comme du XML, sinon comme du YAML. Le nom de la fonction défini dans la configuration générée doit correspondre au nom en cours de création. Si `create_command` se termine avec un status différent de zéro, l&#39;instruction échoue avec une exception qui inclut le code de sortie et la sortie d&#39;erreur standard du driver.

`drop_command`, lorsqu&#39;elle est présente, est invoquée de la même manière (sans corps de code sur stdin) lorsque la fonction est supprimée.

<div id="creating-a-function-with-a-driver">
  ### Créer une fonction
</div>

```sql
CREATE [OR REPLACE] FUNCTION [IF NOT EXISTS] name [ON CLUSTER cluster]
    ARGUMENTS (a UInt8, b String) RETURNS UInt64
    ENGINE = DriverName(key1 = 'value1', key2 = 42)
    AS '...code body...'
```

ClickHouse exécute le `create_command` du driver, écrit la configuration générée dans [`dynamic_user_defined_executable_functions_path`](../../operations/server-configuration-parameters/settings.md#dynamic_user_defined_executable_functions_path), puis le chargeur existant des UDF exécutables la récupère. La fonction peut ensuite être appelée comme n’importe quelle autre fonction.

<div id="dropping-a-function-with-a-driver">
  ### Suppression d’une fonction
</div>

```sql
DROP FUNCTION [IF EXISTS] name [ON CLUSTER cluster]
```

`DROP FUNCTION` appelle le `drop_command` du driver (s&#39;il est présent), supprime la configuration dynamique générée et le répertoire de travail propre à chaque fonction, recharge le chargeur des UDF exécutables et supprime la requête persistée.

<div id="driver-persistence-and-restart">
  ### Persistance et redémarrage
</div>

La requête d&#39;origine est enregistrée sous la forme d&#39;une instruction `ATTACH FUNCTION ...` dans le répertoire des objets SQL définis par l&#39;utilisateur, afin que la fonction survive au redémarrage du serveur. Au démarrage, les configurations générées dans [`dynamic_user_defined_executable_functions_path`](../../operations/server-configuration-parameters/settings.md#dynamic_user_defined_executable_functions_path) sont chargées directement, sans réexécuter le driver. Si une instruction `ATTACH FUNCTION` enregistrée n&#39;a pas de configuration générée correspondante (par exemple, si le répertoire dynamique a été perdu), le driver est réexécuté pour la recréer.

<div id="driver-limitations">
  ### Limitations
</div>

* La fonctionnalité est expérimentale et conditionnée par `allow_experimental_executable_udf_drivers`.
* Les fonctions basées sur un driver ne sont pas prises en charge avec le stockage répliqué des fonctions définies par l’utilisateur (`ON CLUSTER` et `<user_defined_zookeeper_path>`), car seule la requête d’origine est répliquée, pas les artefacts générés.
* Le `RESTORE` d’une fonction basée sur un driver sauvegardée conserve la requête, mais ne relance pas le driver ; la configuration générée est matérialisée plus tard lors de la récupération au redémarrage.

<div id="example-c-drivers">
  ### Exemple de drivers C
</div>

L’arborescence des sources inclut des drivers de démonstration dans `programs/server/user_defined_executable_function_drivers_config.d/` qui compilent et exécutent le corps d’une fonction C. Ce sont des exemples et ils **ne sont pas installés via les paquets** :

* `DockerC` - compile et exécute le code dans des conteneurs Docker isolés (`--network=none --read-only --cap-drop=ALL --security-opt=no-new-privileges`, avec en plus des limites de mémoire/CPU/PID), en produisant une UDF `executable_pool`.
* `GVisorC` - une variante qui exécute le binaire compilé avec le runtime `runsc` de [gVisor](https://gvisor.dev/).
* `UnsafeC` - compile et exécute le code directement sur l’hôte sans sandbox. Comme son nom l’indique, il ne fournit aucune isolation et est destiné uniquement aux environnements de confiance et aux tests.

Ces drivers d’exemple servent de point de départ ; vérifiez et renforcez l’isolation adaptée à votre environnement avant de les exposer à des utilisateurs non fiables.

<div id="related-content">
  ## Voir aussi
</div>

* [Fonctions définies par l’utilisateur dans ClickHouse Cloud](https://clickhouse.com/blog/user-defined-functions-clickhouse-udfs)