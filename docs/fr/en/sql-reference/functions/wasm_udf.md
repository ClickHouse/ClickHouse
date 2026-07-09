---
description: 'Documentation des fonctions utilisateur WebAssembly'
sidebar_label: 'UDF WebAssembly'
slug: /sql-reference/functions/wasm_udf
title: 'Fonctions utilisateur WebAssembly'
doc_type: 'guide'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

<div id="webassembly-user-defined-functions">
  # Fonctions définies par l’utilisateur en WebAssembly
</div>

ClickHouse prend en charge la création de fonctions définies par l’utilisateur (UDF) écrites en WebAssembly. Vous pouvez ainsi exécuter une logique personnalisée écrite dans des langages tels que Rust, C, C++ ou d’autres, en les compilant en modules WebAssembly.

<CloudNotSupportedBadge />

<ExperimentalBadge />

<div id="overview">
  ## Vue d’ensemble
</div>

Un module WebAssembly est un fichier binaire compilé qui contient une ou plusieurs fonctions pouvant être appelées par ClickHouse.
Considérez un module comme une bibliothèque ou un objet partagé que vous chargez une fois pour le réutiliser de nombreuses fois.

Les modules WebAssembly contenant des UDFs peuvent être écrits dans n’importe quel langage pouvant être compilé en WebAssembly, comme Rust, C ou C++.

Le code compilé en WebAssembly (« guest ») et exécuté par ClickHouse (« host ») s’exécute dans un environnement isolé n’ayant accès qu’à un espace mémoire dédié.

Le code guest exporte des fonctions que ClickHouse peut invoquer - il s’agit notamment des fonctions qui implémentent votre logique personnalisée (utilisées pour définir des UDFs), ainsi que des fonctions de support requises pour la gestion de la mémoire et l’échange de données entre ClickHouse et le code WebAssembly.

Votre code doit être compilé en WebAssembly « freestanding » (alias `wasm32-unknown-unknown`), sans dépendance à un système d’exploitation ni à une bibliothèque standard. De plus, seule la cible WebAssembly 32 bits par défaut est prise en charge (pas d’extension `wasm64`).
Le module doit suivre l’un des protocoles de communication pris en charge (ABI) pour interagir avec ClickHouse.

Une fois compilé, le code binaire du module est chargé dans ClickHouse en l’insérant dans la table `system.webassembly_modules`.
Après cela, vous pouvez créer des UDFs qui font référence à des fonctions exportées par le module à l’aide de l’instruction `CREATE FUNCTION ... LANGUAGE WASM`.

<div id="prerequisites">
  ## Prérequis
</div>

Activez la prise en charge de WebAssembly dans la configuration de ClickHouse :

```xml
<clickhouse>
    <allow_experimental_webassembly_udf>true</allow_experimental_webassembly_udf>
    <webassembly_udf_engine>wasmtime</webassembly_udf_engine>
</clickhouse>
```

Implémentations de moteur disponibles :

* `wasmtime` (par défaut, recommandé) — utilise [WasmTime](https://github.com/bytecodealliance/wasmtime)
* `wasmedge` — utilise [WasmEdge](https://github.com/WasmEdge/WasmEdge)

<div id="quick-start">
  ## Démarrage rapide
</div>

Cet exemple présente le processus complet de création d’une UDF WebAssembly en implémentant un programme de calcul de la [conjecture de Collatz](https://en.wikipedia.org/wiki/Collatz_conjecture).

Nous écrirons le code au format texte WebAssembly (WAT), qui est une représentation de WebAssembly lisible par l’être humain ; aucun langage de programmation n’est donc nécessaire à ce stade.
ClickHouse exige que le module soit au format binaire ; nous utiliserons donc le transpileur pour convertir le WAT en WASM.
Pour effectuer cette conversion, vous pouvez utiliser `wat2wasm` du [WebAssembly Binary Toolkit (WABT)](https://github.com/WebAssembly/wabt) ou la commande `parse` de [wasm-tools](https://github.com/bytecodealliance/wasm-tools).

```bash
cat << 'EOF' | wasm-tools parse | clickhouse client -q "INSERT INTO system.webassembly_modules (name, code) SELECT 'collatz', code FROM input('code String') FORMAT RawBlob"
(module
  (func $next (param $n i32) (result i32)
    local.get $n i32.const 1 i32.and
    (if (result i32)
      (then local.get $n i32.const 3 i32.mul i32.const 1 i32.add)
      (else local.get $n i32.const 2 i32.div_u)))
  (func $steps (export "steps") (param $n i32) (result i32)
    (local $count i32)
    local.get $n i32.const 1 i32.lt_u
    (if (then i32.const 0 return))
    (block $done (loop $loop
      local.get $n i32.const 1 i32.eq br_if $done
      local.get $n call $next local.set $n
      local.get $count i32.const 1 i32.add local.set $count
      br $loop))
    local.get $count)
)
EOF
```

Dans l’extrait ci-dessus, nous envoyons le code binaire WASM directement au clickhouse client à l’aide de `FORMAT RawBlob` afin de l’insérer dans la table `system.webassembly_modules`.

Nous définissons ensuite l’UDF qui fait référence à la fonction `steps` exportée par le module :

```sql
CREATE FUNCTION collatz_steps LANGUAGE WASM ARGUMENTS (n UInt32) RETURNS UInt32 FROM 'collatz' :: 'steps';
```

Notez que nous indiquons le nom de la fonction du module après `::`, car il diffère de celui de l’UDF.

Nous pouvons maintenant utiliser la fonction `collatz_steps` dans nos requêtes :

```sql
SELECT groupArray(collatz_steps(number :: UInt32))
FROM numbers(1, 100)
FORMAT TSV
```

La colonne `number` est convertie explicitement en `UInt32`, car les fonctions WebAssembly exigent une correspondance exacte avec la signature de types spécifiée dans l’instruction `CREATE FUNCTION`.

Dans le résultat, nous obtenons la séquence des étapes de Collatz pour les nombres de 1 à 100, correspondant à la suite [A006577 de l’OEIS](https://oeis.org/A006577).

```text
[0,1,7,2,5,8,16,3,19,6,14,9,9,17,17,4,12,20,20,7,7,15,15,10,23,10,111,18,18,18,106,5,26,13,13,21,21,21,34,8,109,8,29,16,16,16,104,11,24,24,24,11,11,112,112,19,32,19,32,19,19,107,107,6,27,27,27,14,14,14,102,22,115,22,14,22,22,35,35,9,22,110,110,9,9,30,30,17,30,17,92,17,17,105,105,12,118,25,25,25]
```

<div id="manage-wasm-modules-via-system-table">
  ## Gérer les modules WASM via la table système
</div>

Les modules WebAssembly sont stockés dans la table `system.webassembly_modules`, qui a la structure suivante :

* **Colonnes**
  * `name` String — Nom du module. Non vide, caractères alphanumériques et underscores uniquement.
  * `code` String — Code binaire WASM brut. Écriture seule, les lectures renvoient une chaîne vide.
  * `hash` UInt256 — SHA256 du binaire du module (zéro s&#39;il est présent sur le disque mais n&#39;est pas encore chargé).

La gestion des modules s&#39;effectue au moyen d&#39;opérations SQL standard sur cette table :

<div id="insert-a-module">
  ### Ajouter un module
</div>

```sql
INSERT INTO system.webassembly_modules (name, code)
SELECT 'my_module', base64Decode('AGFzbQEAAAA...');
```

Vous pouvez également fournir une empreinte d’intégrité :

```sql
INSERT INTO system.webassembly_modules (name, code, hash)
SELECT 'my_module', base64Decode('...'), reinterpretAsUInt256(unhex('369f...c57d'));
```

Si le hash fourni ne correspond pas au SHA256 calculé du code du module, l&#39;insertion échoue. Cela peut être utile lors du chargement de modules depuis des sources externes comme S3 ou HTTP.

<div id="distribute-a-module-across-a-cluster">
  ### Distribuer un module sur un cluster
</div>

`system.webassembly_modules` est une table propre à chaque instance — un `INSERT` n’est appliqué que sur la réplique qui gère la connection. Il n’existe pas de forme `ON CLUSTER` de l’instruction `INSERT`, donc un `CREATE FUNCTION ... ON CLUSTER` exécuté ensuite échouera sur les répliques qui ne disposent pas du module :

```text
Code: 674. DB::Exception: WebAssembly module 'collatz' not found:
while adding user defined function `collatz_steps`. (RESOURCE_NOT_FOUND)
```

Pour distribuer un `insert` à chaque nœud, écrivez dans la fonction de table `cluster` plutôt que dans la table locale `system.webassembly_modules` :

```bash
cat collatz.wasm | clickhouse client -q "
  INSERT INTO FUNCTION cluster('default', 'system', 'webassembly_modules') (name, code)
  SELECT 'collatz', code FROM input('code String') FORMAT RawBlob"
```

:::note
Ce schéma repose sur le fait que le chemin d’écriture distribuée sous-jacent passe par chaque réplique de chaque shard, ce qui ne se produit que lorsque le cluster est configuré avec `internal_replication=false`. Avec `internal_replication=true` (la valeur par défaut des clusters qui utilisent `ReplicatedMergeTree` pour assurer eux-mêmes la réplication), l’insert est envoyé à une seule réplique saine par shard, et `system.webassembly_modules` n’est pas répliqué par ce chemin ; certaines répliques n’auront donc toujours pas le module. Dans cette configuration, vous devez effectuer un insert sur chaque réplique individuellement, par exemple en parcourant `system.clusters` et en écrivant via `remote(...)` pour chaque hôte, ou en copiant le binaire dans `user_scripts/wasm/` sur chaque hôte.

Vous pouvez vérifier `internal_replication` d’un cluster avec `SELECT cluster, shard_num, internal_replication FROM system.clusters`.
:::

Après cet insert distribué, le module est présent sur chaque réplique et `CREATE FUNCTION ... ON CLUSTER` réussit :

```sql
CREATE FUNCTION collatz_steps ON CLUSTER 'default'
LANGUAGE WASM FROM 'collatz' :: 'steps'
ARGUMENTS (n UInt32) RETURNS UInt32;
```

Vous pouvez vérifier que le module est chargé partout à l’aide de `clusterAllReplicas` :

```sql
SELECT hostName(), name FROM clusterAllReplicas('default', system.webassembly_modules) WHERE name = 'collatz';
```

Les insertions dans `system.webassembly_modules` sont idempotentes pour une même paire `(name, hash)` ; relancer l’insertion distribuée est donc sans risque et constitue un moyen raisonnable de rétablir l’état après le remplacement d’une réplique. Notez que les serveurs nouvellement ajoutés ne reçoivent pas rétroactivement les modules existants — vous devez relancer l’insertion sur le cluster mis à jour, ou placer le binaire dans le répertoire `user_scripts/wasm/` sur le nouvel hôte.

<div id="list-modules">
  ### Lister les modules
</div>

```sql
SELECT name, lower(hex(reinterpretAsFixedString(hash))) AS sha256 FROM system.webassembly_modules

   ┌─name────┬─sha256───────────────────────────────────────────────────────────┐
1. │ collatz │ a084a10b7b5cb07db198bc93bf1f3c1f8cb8ef279df7a4f6b66b1cdd55d79c48 │
   └─────────┴──────────────────────────────────────────────────────────────────┘
```

<div id="delete-a-module">
  ### Supprimer un module
</div>

La suppression s’effectue à l’aide de l’instruction `DELETE FROM system.webassembly_modules WHERE name = '...'`.
Le prédicat doit être soit `name = 'literal'` pour une correspondance exacte, soit `name LIKE 'pattern'` pour supprimer tous les modules dont le nom correspond au motif ; aucune autre forme n’est acceptée.

```sql
DELETE FROM system.webassembly_modules WHERE name = 'collatz';

-- Bulk-delete every module whose name starts with `tmp_` (literal underscore is escaped as `\_`):
DELETE FROM system.webassembly_modules WHERE name LIKE 'tmp\_%';
```

Si des UDFs existantes font référence à l’un des modules correspondants, la suppression échoue ; vous devez donc d’abord supprimer ces UDFs.

<div id="create-a-webassembly-udf">
  ## Créer une UDF WebAssembly
</div>

**Syntaxe**:

```sql
CREATE [OR REPLACE] FUNCTION function_name
LANGUAGE WASM
FROM 'module_name' [:: 'source_function_name']
ARGUMENTS ( [name type[, ...]] | [type[, ...]] )
RETURNS return_type
[ABI ROW_DIRECT | ABI BUFFERED_V1 | ABI ASSEMBLYSCRIPT]
[DETERMINISTIC]
[SHA256_HASH 'hex']
[SETTINGS key = value[, ...]];
```

**Paramètres** :

* `function_name` : Nom de la fonction dans ClickHouse. Il peut être différent du nom de la fonction exportée dans le module.
* `FROM 'module_name' :: 'source_function_name'` : Nom du module WASM chargé et nom de la fonction du module WASM à utiliser (par défaut : function&#95;name)
* `ARGUMENTS` : Liste des noms et des types des arguments (les noms sont facultatifs et utilisés pour les formats de sérialisation prenant en charge les champs nommés)
* `ABI` : Version de l’Application Binary Interface
  * `ROW_DIRECT` : Correspondance de types directe, traitement ligne par ligne
  * `BUFFERED_V1` : Traitement par blocs avec sérialisation
  * `ASSEMBLYSCRIPT` : Traitement ligne par ligne pour les modules produits par le compilateur [AssemblyScript](https://www.assemblyscript.org). Les types numériques correspondent aux primitives d’AssemblyScript ; le type ClickHouse `String` correspond au type `string` d’AssemblyScript.
* `DETERMINISTIC` : Déclare la fonction comme déterministe — elle renvoie toujours le même résultat pour la même entrée. Lorsqu’il est spécifié, ClickHouse peut précalculer les appels dont tous les arguments sont des constantes : la fonction est évaluée une seule fois au moment de l’analyse de la requête, puis le résultat est réutilisé pour chaque ligne.
* `SHA256_HASH` : Hachage attendu du module pour vérification (renseigné automatiquement s’il est omis) ; peut être utilisé pour garantir que le bon module WASM est chargé sur différentes répliques.
* `SETTINGS` : Paramètres propres à la fonction
  * `serialization_format` String — Format de sérialisation pour l’ABI lorsque celle-ci l’exige. Valeurs prises en charge : `MsgPack`, `JSONEachRow`, `CSV`, `TSV`, `TSVRaw`, `RowBinary` et `Buffers`. Valeur par défaut : `MsgPack`. Les formats basés sur des blocs tels que `Buffers` doivent renvoyer une seule colonne dont le type correspond à la signature de fonction déclarée.
  * `webassembly_udf_enable_fuel` Bool — Active un budget fini de fuel pour la fonction. Valeur par défaut : `true`. Lorsque `false`, le paramètre de requête `webassembly_udf_max_fuel` est ignoré pour cette fonction. La désactivation des limites de fuel peut améliorer les performances lors de l’utilisation du moteur `wasmtime`. Toutefois, pour du code invité non fiable ou bogué, cela peut accroître le risque d’exécution incontrôlée.

<div id="abis-versions">
  ## Versions d’ABI
</div>

Pour interagir avec ClickHouse, les modules WebAssembly doivent respecter l’une des ABI (Application Binary Interfaces) prises en charge.

* `ROW_DIRECT` : correspondance de types directe (types primitifs `Int32`, `UInt32`, `Int64`, `UInt64`, `Float32`, `Float64` uniquement)
* `BUFFERED_V1` : types complexes avec sérialisation
* `ASSEMBLYSCRIPT` : interop ligne par ligne avec des modules [AssemblyScript](https://www.assemblyscript.org) ; prend en charge les types numériques et `String`.

<div id="abi-row_direct">
  ### ABI ROW_DIRECT
</div>

Appelle directement une fonction WASM exportée pour chaque ligne.

* Les arguments et les types de retour doivent être des types numériques `Int32/UInt32/Int64/UInt64/Float32/Float64/Int128/UInt128`.
* Les chaînes de caractères ne sont pas prises en charge dans cette ABI.
* Les signatures doivent correspondre à l’export WASM (`i32/i64/f32/f64/v128`).
* Aucune fonction de support n’a besoin d’être exportée par le module.

Par exemple, une fonction avec la signature :

```
(func (param i32 i64 f32) (result f64) ...)
```

Se crée comme suit :

```sql
CREATE FUNCTION my_func ARGUMENTS (Int32, UInt64, Float32) RETURNS Float64 ...
```

WebAssembly ne fait pas de distinction entre les arguments signés et non signés, mais utilise plutôt différentes instructions pour interpréter les valeurs. Ainsi, la taille de l&#39;argument doit correspondre exactement, tandis que le fait qu&#39;il soit signé ou non est déterminé par les opérations à l&#39;intérieur de la fonction.

<div id="abi-buffered_v1">
  ### ABI BUFFERED_V1
</div>

:::note
Cette ABI est expérimentale et susceptible d’évoluer dans les versions futures.
:::

Traite des blocks entiers en une seule fois à l’aide d’une (dé)sérialisation via la mémoire WASM. Prend en charge tous les types d’argument et de retour.

Les données sérialisées sont copiées dans la mémoire WASM et transmises à la fonction UDF sous la forme d’un pointeur vers le buffer (qui se compose d’un pointeur vers les données et de la taille des données), avec le nombre de lignes en entrée. Ainsi, la fonction définie par l’utilisateur côté WASM accepte toujours deux arguments `i32` et renvoie une unique valeur `i32`.
Le code guest traite les données et renvoie un pointeur vers le buffer de résultat contenant les données de résultat sérialisées.

Le code guest doit fournir deux fonctions pour créer et détruire ces buffers.

```
(module
  ;; Allocate a new buffer of specified size
  ;; Returns: handle to Buffer structure (not direct data pointer!) with pointer to data and size
  (func (export "clickhouse_create_buffer")
    (param $size i32)    ;; Size of data to allocate
    (result i32))        ;; Returns buffer handle with enough space

  ;; Free a buffer by its handle
  (func (export "clickhouse_destroy_buffer")
    (param $handle i32)  ;; Buffer handle to free
    (result))            ;; No return value

    ;; User-defined function
    (func (export "user_defined_function1")
      (param $input_buffer_handle i32)  ;; Input buffer handle
      (param $n i32)                    ;; Number of rows in input
      (result i32))                     ;; Returns output buffer handle
)
```

Exemple de définitions en C :

```c
typedef struct {
    uint8_t * data;
    uint32_t size;
} ClickhouseBuffer;

ClickhouseBuffer * clickhouse_create_buffer(uint32_t size) { /* ... */ }

void clickhouse_destroy_buffer(ClickhouseBuffer * data) { /* ... */ }

/// Example user-defined functions
ClickhouseBuffer * user_defined_function1(ClickhouseBuffer * span, uint32_t n) { /* ... */ }
ClickhouseBuffer * user_defined_function2(ClickhouseBuffer * span, uint32_t n) { /* ... */ }
```

<div id="abi-assemblyscript">
  ### ABI ASSEMBLYSCRIPT
</div>

Concerne les modules produits par le compilateur [AssemblyScript](https://www.assemblyscript.org). Chaque ligne déclenche un appel à la fonction exportée, en faisant correspondre les valeurs ClickHouse aux types primitifs et aux objets chaîne d’AssemblyScript.

**Types pris en charge** :

* Numériques : `Int8`/`UInt8`, `Int16`/`UInt16` (élargis en `i32` à l’interface), `Int32`/`UInt32`, `Int64`/`UInt64`, `Float32`, `Float64`

* `String` — correspond au type `string` d’AssemblyScript (UTF-16 dans la mémoire WASM). ClickHouse gère automatiquement la conversion UTF-8 ↔ UTF-16.

* Les classes AssemblyScript personnalisées ne sont pas prises en charge comme types d’argument ou de retour — leurs identifiants de classe à l’exécution ne sont pas stables d’une compilation à l’autre (voir [AssemblyScript#2982](https://github.com/AssemblyScript/assemblyscript/issues/2982)).

**Exigences du module** :

Le module doit être compilé avec le runtime Managed d’AssemblyScript afin que `__new`, `__pin` et `__unpin` soient exportés. La gestion standard des chaînes en entrée et en sortie s’appuie sur ces éléments. Invocation recommandée :

```bash
asc src.ts --runtime incremental --exportRuntime -o src.wasm
```

AssemblyScript importe également `env.abort` pour les erreurs d’exécution du runtime (mémoire insuffisante, vérifications de limites, etc.). ClickHouse fournit automatiquement cet import : lorsqu’un `abort` est déclenché, la requête active échoue avec une exception `WASM_ERROR` qui inclut le message AssemblyScript décodé et l’emplacement dans le code source.

**Exemple**:

```typescript
// src.ts
export function add(a: u32, b: u32): u32 {
  return a + b;
}

export function greet(name: string): string {
  return "Hello, " + name + "!";
}
```

Après avoir compilé avec `asc` et chargé le fichier `.wasm` obtenu dans `system.webassembly_modules`, déclarez les UDFs comme suit :

```sql
CREATE FUNCTION as_add
    LANGUAGE WASM ABI ASSEMBLYSCRIPT
    FROM 'as_example' :: 'add'
    ARGUMENTS (a UInt32, b UInt32) RETURNS UInt32;

CREATE FUNCTION as_greet
    LANGUAGE WASM ABI ASSEMBLYSCRIPT
    FROM 'as_example' :: 'greet'
    ARGUMENTS (name String) RETURNS String;
```

<div id="note-for-developing-udfs-in-rust">
  ### Remarque sur le développement de UDF en Rust
</div>

Pour les programmes Rust, nous fournissons une crate utilitaire [clickhouse-wasm-udf](https://crates.io/crates/clickhouse-wasm-udf) pour simplifier le développement de WebAssembly UDF pour ClickHouse. Cette crate fournit des fonctions de gestion de la mémoire, vous n’avez donc pas besoin d’implémenter manuellement les fonctions `clickhouse_create_buffer` et `clickhouse_destroy_buffer` ; ajoutez simplement la crate comme dépendance. Elle inclut également les macros `#[clickhouse_wasm_udf]` pour encapsuler vos fonctions Rust classiques dans le format ABI requis.

Avec cette crate, vous pouvez écrire des UDF comme ceci :

```rust

use clickhouse_wasm_udf_bindgen::clickhouse_udf;

#[clickhouse_udf]
pub fn some_udf(data: String) -> HashMap<String, String> {
    // Your implementation here
}

```

Les macros généreront une fonction wrapper qui accepte et renvoie des structures de buffer, et assureront automatiquement la sérialisation/désérialisation à l’aide de `serde`.

<div id="host-api-available-to-modules">
  ## API hôte disponible pour les modules
</div>

Les fonctions hôtes suivantes peuvent être importées et utilisées par les modules :

* `clickhouse_server_version() -> i64` — renvoie la version du serveur ClickHouse sous forme d’entier (par ex. 25011001 pour v25.11.1.1).
* `clickhouse_throw(ptr: i32, size: i32)` — déclenche une erreur avec le message fourni. Accepte un pointeur vers l’emplacement mémoire contenant la chaîne du message d’erreur, ainsi que la taille de cette chaîne.
* `clickhouse_log(ptr: i32, size: i32)` — consigne un message dans le journal texte du serveur ClickHouse.
* `clickhouse_random(ptr: i32, size: i32)` — remplit la mémoire avec des octets aléatoires.
* `env.abort(message: i32, fileName: i32, line: i32, column: i32)` — fournie pour les modules compatibles avec AssemblyScript. Son appel (ou le déclenchement d’un trap du runtime AssemblyScript qui l’appelle) met fin à l’exécution de la UDF avec une exception `WASM_ERROR` contenant le message décodé et l’emplacement dans le code source. Les modules qui n’importent pas `env.abort` ne sont pas affectés.

<div id="settings">
  ## Paramètres
</div>

Les paramètres suivants, au niveau de la requête, contrôlent l’exécution des UDF WebAssembly :

* `webassembly_udf_max_fuel` — Limite de fuel par exécution d’une instance d’UDF WebAssembly. Chaque instruction WebAssembly consomme une certaine quantité de fuel. La valeur est multipliée par 1024 avant d’être transmise au runtime ; ainsi, `webassembly_udf_max_fuel = 1` correspond à environ 1024 unités de fuel. Définissez-la sur 0 pour supprimer toute limite. S’applique uniquement aux fonctions dont le paramètre propre à la fonction `webassembly_udf_enable_fuel` est true, ce qui est la valeur par défaut.

* `webassembly_udf_max_memory` — Limite de mémoire, en octets, par instance d’UDF WebAssembly.

* `webassembly_udf_max_input_block_size` — Nombre maximal de lignes transmises à une UDF WebAssembly dans un seul bloc. Définissez-le sur 0 pour traiter toutes les lignes en une seule fois.

* `webassembly_udf_max_instances` — Nombre maximal d’instances d’UDF WebAssembly pouvant s’exécuter en parallèle par fonction.

Exemple d’utilisation :

```sql
SET webassembly_udf_max_fuel = 200000;
SELECT my_wasm_udf(column) FROM table;
```

<div id="see-also">
  ## Voir aussi
</div>

* [Présentation des UDF ClickHouse](/fr/sql-reference/functions/udf)