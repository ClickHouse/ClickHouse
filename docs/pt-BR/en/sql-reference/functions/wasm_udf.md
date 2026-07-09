---
description: 'Documentação para funções definidas pelo usuário em WebAssembly'
sidebar_label: 'UDFs em WebAssembly'
slug: /sql-reference/functions/wasm_udf
title: 'Funções definidas pelo usuário em WebAssembly'
doc_type: 'guide'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

<div id="webassembly-user-defined-functions">
  # Funções Definidas pelo Usuário em WebAssembly
</div>

O ClickHouse oferece suporte à criação de Funções Definidas pelo Usuário (UDFs) escritas em WebAssembly. Isso permite executar lógica personalizada escrita em linguagens como Rust, C, C++ e outras, compilando-a em módulos do WebAssembly.

<CloudNotSupportedBadge />

<ExperimentalBadge />

<div id="overview">
  ## Visão geral
</div>

Um módulo WebAssembly é um arquivo binário compilado que contém uma ou mais funções que podem ser chamadas pelo ClickHouse.
Pense em um módulo como uma biblioteca ou objeto compartilhado que você carrega uma vez e reutiliza várias vezes.

Um módulo WebAssembly que contém UDFs pode ser escrito em qualquer linguagem que possa ser compilada para WebAssembly, como Rust, C ou C++.

O código compilado para WebAssembly (código &quot;guest&quot;) e executado pelo ClickHouse (&quot;host&quot;) roda em um ambiente isolado, com acesso apenas a um espaço de memória dedicado.

O código guest exporta funções que o ClickHouse pode invocar — isso inclui tanto as funções que implementam sua lógica personalizada (usadas para definir UDFs) quanto as funções de suporte necessárias para o gerenciamento de memória e a troca de dados entre o ClickHouse e o código WebAssembly.

Seu código deve ser compilado para WebAssembly &quot;freestanding&quot; (também conhecido como `wasm32-unknown-unknown`), sem nenhuma dependência de sistema operacional ou biblioteca padrão. Além disso, apenas o destino padrão de WebAssembly de 32 bits é compatível (sem a extensão `wasm64`).
O módulo deve seguir um dos protocolos de comunicação compatíveis (ABIs) para interagir com o ClickHouse.

Depois de compilado, o código binário do módulo é carregado no ClickHouse inserindo-o na tabela `system.webassembly_modules`.
Depois disso, você pode criar UDFs que fazem referência a funções exportadas pelo módulo usando a instrução `CREATE FUNCTION ... LANGUAGE WASM`.

<div id="prerequisites">
  ## Pré-requisitos
</div>

Ative o suporte a WebAssembly na configuração do ClickHouse:

```xml
<clickhouse>
    <allow_experimental_webassembly_udf>true</allow_experimental_webassembly_udf>
    <webassembly_udf_engine>wasmtime</webassembly_udf_engine>
</clickhouse>
```

Implementações de engine disponíveis:

* `wasmtime` (padrão, recomendado) — usa [WasmTime](https://github.com/bytecodealliance/wasmtime)
* `wasmedge` — usa [WasmEdge](https://github.com/WasmEdge/WasmEdge)

<div id="quick-start">
  ## Quick Start
</div>

Este exemplo demonstra o fluxo de trabalho completo para criar uma WebAssembly UDF implementando a calculadora da [conjectura de Collatz](https://en.wikipedia.org/wiki/Collatz_conjecture).

Vamos escrever o código no formato de texto do WebAssembly (WAT), que é uma representação legível por humanos do WebAssembly, portanto nenhuma linguagem de programação é necessária nesta etapa.
O ClickHouse exige que o módulo esteja em formato binário, então usaremos o transpiler para converter WAT em WASM.
Para realizar essa conversão, você pode usar `wat2wasm` do [WebAssembly Binary Toolkit (WABT)](https://github.com/WebAssembly/wabt) ou o comando `parse` do [wasm-tools](https://github.com/bytecodealliance/wasm-tools).

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

No trecho acima, enviamos o código binário WASM diretamente para o clickhouse client usando `FORMAT RawBlob` para inseri-lo na tabela `system.webassembly_modules`.

Em seguida, definimos a UDF que faz referência à função `steps` exportada pelo módulo:

```sql
CREATE FUNCTION collatz_steps LANGUAGE WASM ARGUMENTS (n UInt32) RETURNS UInt32 FROM 'collatz' :: 'steps';
```

Observe que especificamos o nome da função do módulo após `::`, pois ele difere do nome da UDF.

Agora podemos usar a função `collatz_steps` em nossas consultas:

```sql
SELECT groupArray(collatz_steps(number :: UInt32))
FROM numbers(1, 100)
FORMAT TSV
```

A coluna `number` é convertida explicitamente para `UInt32`, porque as funções WebAssembly exigem correspondência exata com os tipos especificados na assinatura da instrução `CREATE FUNCTION`.

Como resultado, obtivemos a sequência de passos de Collatz para os números de 1 a 100, que corresponde à sequência [A006577 from the OEIS](https://oeis.org/A006577).

```text
[0,1,7,2,5,8,16,3,19,6,14,9,9,17,17,4,12,20,20,7,7,15,15,10,23,10,111,18,18,18,106,5,26,13,13,21,21,21,34,8,109,8,29,16,16,16,104,11,24,24,24,11,11,112,112,19,32,19,32,19,19,107,107,6,27,27,27,14,14,14,102,22,115,22,14,22,22,35,35,9,22,110,110,9,9,30,30,17,30,17,92,17,17,105,105,12,118,25,25,25]
```

<div id="manage-wasm-modules-via-system-table">
  ## Gerencie módulos WASM por meio da tabela do sistema
</div>

Os módulos WebAssembly são armazenados na tabela `system.webassembly_modules`, com a seguinte estrutura:

* **Colunas**
  * `name` String — Nome do módulo. Não pode ser vazio; apenas caracteres de palavra.
  * `code` String — Código WASM binário bruto. Somente para gravação; as leituras retornam string vazia.
  * `hash` UInt256 — SHA256 do binário do módulo (zero se estiver presente no disco, mas ainda não tiver sido carregado).

O gerenciamento de módulos é feito por meio de operações SQL padrão nessa tabela:

<div id="insert-a-module">
  ### Inserir um módulo
</div>

```sql
INSERT INTO system.webassembly_modules (name, code)
SELECT 'my_module', base64Decode('AGFzbQEAAAA...');
```

Opcionalmente, informe o hash de integridade:

```sql
INSERT INTO system.webassembly_modules (name, code, hash)
SELECT 'my_module', base64Decode('...'), reinterpretAsUInt256(unhex('369f...c57d'));
```

Se o hash fornecido não corresponder ao SHA256 calculado para o código do módulo, a inserção falhará. Isso pode ser útil ao carregar módulos de fontes externas, como S3 ou HTTP.

<div id="distribute-a-module-across-a-cluster">
  ### Distribua um módulo em todo o cluster
</div>

`system.webassembly_modules` é uma tabela por instância — um `INSERT` é aplicado apenas à réplica que está atendendo à conexão. Não existe uma forma `ON CLUSTER` da instrução `INSERT`, portanto um `CREATE FUNCTION ... ON CLUSTER` subsequente falhará nas réplicas que não têm o módulo:

```text
Code: 674. DB::Exception: WebAssembly module 'collatz' not found:
while adding user defined function `collatz_steps`. (RESOURCE_NOT_FOUND)
```

Para distribuir um `insert` para todos os nós, grave na função de tabela `cluster` em vez de na tabela local `system.webassembly_modules`:

```bash
cat collatz.wasm | clickhouse client -q "
  INSERT INTO FUNCTION cluster('default', 'system', 'webassembly_modules') (name, code)
  SELECT 'collatz', code FROM input('code String') FORMAT RawBlob"
```

:::note
Esse padrão depende de o caminho subjacente de gravação distribuída passar por cada réplica em cada shard, o que só acontece quando o cluster está configurado com `internal_replication=false`. Com `internal_replication=true` (o padrão para clusters que usam `ReplicatedMergeTree` para fazer a replicação por conta própria), o insert é enviado para uma única réplica saudável por shard, e `system.webassembly_modules` não é replicado por esse caminho — portanto, algumas réplicas continuarão sem o módulo. Nessa configuração, você precisa fazer o insert em cada réplica individualmente, por exemplo iterando sobre `system.clusters` e gravando via `remote(...)` por host, ou copiando o binário para `user_scripts/wasm/` em cada host.

Você pode inspecionar `internal_replication` de um cluster com `SELECT cluster, shard_num, internal_replication FROM system.clusters`.
:::

Após o insert com fan-out, o módulo passa a estar presente em cada réplica, e `CREATE FUNCTION ... ON CLUSTER` é executado com sucesso:

```sql
CREATE FUNCTION collatz_steps ON CLUSTER 'default'
LANGUAGE WASM FROM 'collatz' :: 'steps'
ARGUMENTS (n UInt32) RETURNS UInt32;
```

Você pode verificar se o módulo está carregado em todos os nós com `clusterAllReplicas`:

```sql
SELECT hostName(), name FROM clusterAllReplicas('default', system.webassembly_modules) WHERE name = 'collatz';
```

As inserções em `system.webassembly_modules` são idempotentes para o mesmo par `(name, hash)`; portanto, executar novamente a inserção distribuída é seguro e é uma forma razoável de reparar o estado depois que uma réplica for substituída. Observe que servidores recém-adicionados não recebem retroativamente os módulos existentes — você deve executar novamente a inserção no cluster atualizado ou colocar o binário no diretório `user_scripts/wasm/` do novo servidor.

<div id="list-modules">
  ### Listar módulos
</div>

```sql
SELECT name, lower(hex(reinterpretAsFixedString(hash))) AS sha256 FROM system.webassembly_modules

   ┌─name────┬─sha256───────────────────────────────────────────────────────────┐
1. │ collatz │ a084a10b7b5cb07db198bc93bf1f3c1f8cb8ef279df7a4f6b66b1cdd55d79c48 │
   └─────────┴──────────────────────────────────────────────────────────────────┘
```

<div id="delete-a-module">
  ### Excluir um módulo
</div>

A exclusão é realizada pela instrução `DELETE FROM system.webassembly_modules WHERE name = '...'`.
O predicado deve ser `name = 'literal'` para correspondência exata ou `name LIKE 'pattern'` para excluir todos os módulos cujo nome corresponda ao padrão; nenhuma outra forma é aceita.

```sql
DELETE FROM system.webassembly_modules WHERE name = 'collatz';

-- Bulk-delete every module whose name starts with `tmp_` (literal underscore is escaped as `\_`):
DELETE FROM system.webassembly_modules WHERE name LIKE 'tmp\_%';
```

Se alguma UDF existente fizer referência a um dos módulos correspondentes, a exclusão falhará; portanto, será necessário excluir essas UDFs primeiro.

<div id="create-a-webassembly-udf">
  ## Criar uma UDF em WebAssembly
</div>

**Sintaxe**:

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

**Parâmetros**:

* `function_name`: Nome da função no ClickHouse. Pode ser diferente do nome da função exportada no módulo.
* `FROM 'module_name' :: 'source_function_name'`: Nome do módulo WASM carregado e nome da função no módulo WASM a ser usada (o padrão é function&#95;name)
* `ARGUMENTS`: Lista de nomes e tipos de argumentos (os nomes são opcionais e usados em formatos de serialização que oferecem suporte a campos nomeados)
* `ABI`: Versão da Interface Binária de Aplicação
  * `ROW_DIRECT`: Mapeamento direto de tipos, com processamento linha a linha
  * `BUFFERED_V1`: Processamento baseado em blocos com serialização
  * `ASSEMBLYSCRIPT`: Processamento linha a linha para módulos produzidos pelo compilador [AssemblyScript](https://www.assemblyscript.org). Tipos numéricos são mapeados para primitivos do AssemblyScript; `String` do ClickHouse é mapeado para `string` do AssemblyScript.
* `DETERMINISTIC`: Declara a função como determinística — sempre retorna o mesmo resultado para a mesma entrada. Quando especificado, o ClickHouse pode aplicar constant folding a chamadas em que todos os argumentos são constantes: a função é avaliada uma vez durante a análise da consulta, e o resultado é reutilizado para cada linha.
* `SHA256_HASH`: Hash esperado do módulo para verificação (preenchido automaticamente se omitido); pode ser usado para garantir que o módulo WASM correto seja carregado em diferentes réplicas.
* `SETTINGS`: Configurações por função
  * `serialization_format` String — Formato de serialização para a ABI, caso ela exija um. Valores compatíveis: `MsgPack`, `JSONEachRow`, `CSV`, `TSV`, `TSVRaw`, `RowBinary` e `Buffers`. Padrão: `MsgPack`. Formatos baseados em blocos, como `Buffers`, devem retornar uma única coluna cujo tipo corresponda à assinatura declarada da função.
  * `webassembly_udf_enable_fuel` Bool — Habilita um limite finito de fuel para a função. Padrão: `true`. Quando `false`, a configuração no nível da consulta `webassembly_udf_max_fuel` é ignorada para esta função. Desabilitar os limites de fuel pode melhorar o desempenho ao usar o engine `wasmtime`. No entanto, para código guest não confiável ou com bugs, isso pode aumentar o risco de execução descontrolada.

<div id="abis-versions">
  ## Versões de ABI
</div>

Para interagir com o ClickHouse, os módulos WebAssembly devem seguir uma das ABIs (Interfaces Binárias de Aplicação) compatíveis.

* `ROW_DIRECT`: Mapeamento direto de tipos (somente tipos primitivos `Int32`, `UInt32`, `Int64`, `UInt64`, `Float32`, `Float64`)
* `BUFFERED_V1`: Tipos complexos com serialização
* `ASSEMBLYSCRIPT`: Interoperabilidade linha a linha com módulos [AssemblyScript](https://www.assemblyscript.org); oferece suporte a tipos numéricos e `String`.

<div id="abi-row_direct">
  ### ABI ROW_DIRECT
</div>

Chama diretamente uma função WASM exportada para cada linha.

* Argumentos e tipos de retorno devem ser tipos numéricos `Int32/UInt32/Int64/UInt64/Float32/Float64/Int128/UInt128`.
* Strings não são compatíveis com esta ABI.
* As assinaturas devem corresponder à exportação WASM (`i32/i64/f32/f64/v128`).
* Não é necessário que o módulo exporte funções de suporte.

Por exemplo, uma função com a assinatura:

```
(func (param i32 i64 f32) (result f64) ...)
```

Pode ser criado da seguinte forma:

```sql
CREATE FUNCTION my_func ARGUMENTS (Int32, UInt64, Float32) RETURNS Float64 ...
```

O WebAssembly não faz distinção entre argumentos com sinal e sem sinal; em vez disso, usa instruções diferentes para interpretar os valores. Assim, o tamanho do argumento deve corresponder exatamente, enquanto o sinal é determinado pelas operações dentro da função.

<div id="abi-buffered_v1">
  ### ABI BUFFERED_V1
</div>

:::note
Esta ABI é experimental e está sujeita a mudanças em lançamentos futuros.
:::

Processa blocos inteiros de uma só vez usando (de)serialização por meio da memória WASM. Oferece suporte a quaisquer tipos de argumento e retorno.

Os dados serializados são copiados para a memória WASM e passados para a função UDF como um ponteiro para o buffer (que consiste em um ponteiro para os dados e no tamanho desses dados), juntamente com o número de linhas na entrada. Assim, a função definida pelo usuário em WASM sempre aceita dois argumentos `i32` e retorna um único valor `i32`.
O código guest processa os dados e retorna um ponteiro para o buffer de resultado com os dados de saída serializados.

O código guest deve fornecer duas funções para criar e destruir esses buffers.

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

Exemplo de definições em C:

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

Destina-se a módulos produzidos pelo compilador [AssemblyScript](https://www.assemblyscript.org). Cada linha aciona uma chamada para a função exportada, mapeando valores do ClickHouse para tipos primitivos e objetos string do AssemblyScript.

**Tipos compatíveis**:

* Numéricos: `Int8`/`UInt8`, `Int16`/`UInt16` (ampliados para `i32` na fronteira), `Int32`/`UInt32`, `Int64`/`UInt64`, `Float32`, `Float64`

* `String` — é mapeado para `string` do AssemblyScript (UTF-16 na memória WASM). O ClickHouse lida automaticamente com a conversão UTF-8 ↔ UTF-16.

* Classes personalizadas do AssemblyScript não são compatíveis como argumento ou tipo de retorno — seus IDs de classe em tempo de execução não são estáveis entre compilações (consulte [AssemblyScript#2982](https://github.com/AssemblyScript/assemblyscript/issues/2982)).

**Requisitos do módulo**:

O módulo deve ser compilado com o runtime gerenciado do AssemblyScript para que `__new`, `__pin` e `__unpin` sejam exportados. O tratamento padrão de strings de entrada/saída espera isso. A invocação recomendada:

```bash
asc src.ts --runtime incremental --exportRuntime -o src.wasm
```

AssemblyScript também importa `env.abort` para traps do runtime (falta de memória, verificações de limites etc.). O ClickHouse fornece essa importação automaticamente: quando um `abort` é disparado, a consulta ativa falha com uma exceção `WASM_ERROR` que inclui a mensagem decodificada do AssemblyScript e a localização no código-fonte.

**Exemplo**:

```typescript
// src.ts
export function add(a: u32, b: u32): u32 {
  return a + b;
}

export function greet(name: string): string {
  return "Hello, " + name + "!";
}
```

Após compilar com `asc` e carregar o `.wasm` resultante em `system.webassembly_modules`, declare as UDFs da seguinte forma:

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
  ### Observação sobre o desenvolvimento de UDFs em Rust
</div>

Para programas em Rust, fornecemos o crate auxiliar [clickhouse-wasm-udf](https://crates.io/crates/clickhouse-wasm-udf) para simplificar o desenvolvimento de WebAssembly UDFs para o ClickHouse. O crate fornece funções de gerenciamento de memória, então você não precisa implementar manualmente as funções `clickhouse_create_buffer` e `clickhouse_destroy_buffer`; em vez disso, basta adicionar o crate como dependência. Também há macros `#[clickhouse_wasm_udf]` para encapsular suas funções Rust comuns no formato ABI exigido.

Com o crate, você pode escrever UDFs assim:

```rust

use clickhouse_wasm_udf_bindgen::clickhouse_udf;

#[clickhouse_udf]
pub fn some_udf(data: String) -> HashMap<String, String> {
    // Your implementation here
}

```

As macros gerarão uma função wrapper que aceita e retorna estruturas de buffer e cuidará automaticamente da serialização/desserialização usando `serde`.

<div id="host-api-available-to-modules">
  ## API do host disponível para módulos
</div>

As seguintes funções do host podem ser importadas e usadas por módulos:

* `clickhouse_server_version() -> i64` — retorna a versão do servidor ClickHouse como um inteiro (por exemplo, 25011001 para v25.11.1.1).
* `clickhouse_throw(ptr: i32, size: i32)` — lança um erro com a mensagem fornecida. Aceita um ponteiro para o local da memória que contém a string da mensagem de erro e o tamanho da string.
* `clickhouse_log(ptr: i32, size: i32)` — registra uma mensagem no log de texto do servidor ClickHouse.
* `clickhouse_random(ptr: i32, size: i32)` — preenche a memória com bytes aleatórios.
* `env.abort(message: i32, fileName: i32, line: i32, column: i32)` — fornecido para módulos compatíveis com AssemblyScript. Chamá-lo (ou acionar uma interrupção do runtime do AssemblyScript que o chame) encerra a UDF com uma exceção `WASM_ERROR` contendo a mensagem decodificada e a localização no código-fonte. Módulos que não importam `env.abort` não são afetados.

<div id="settings">
  ## Configurações
</div>

As seguintes configurações no nível da consulta controlam a execução de WebAssembly UDF:

* `webassembly_udf_max_fuel` — Limite de fuel por execução de instância de WebAssembly UDF. Cada instrução de WebAssembly consome uma certa quantidade de fuel. O valor é escalado em 1024 antes de ser passado ao runtime, portanto `webassembly_udf_max_fuel = 1` corresponde a aproximadamente 1024 unidades de fuel. Defina como 0 para não haver limite finito. Aplica-se somente a funções cuja configuração por função `webassembly_udf_enable_fuel` seja true, que é o padrão.

* `webassembly_udf_max_memory` — Limite de memória em bytes por instância de WebAssembly UDF.

* `webassembly_udf_max_input_block_size` — Número máximo de linhas passadas para uma WebAssembly UDF em um único bloco. Defina como 0 para processar todas as linhas de uma só vez.

* `webassembly_udf_max_instances` — Número máximo de instâncias de WebAssembly UDF que podem ser executadas em paralelo por função.

Exemplo de uso:

```sql
SET webassembly_udf_max_fuel = 200000;
SELECT my_wasm_udf(column) FROM table;
```

<div id="see-also">
  ## Veja também
</div>

* [Visão geral das UDFs do ClickHouse](/pt-BR/sql-reference/functions/udf)