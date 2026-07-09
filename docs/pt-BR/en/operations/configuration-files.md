---
description: 'Esta página explica como o servidor ClickHouse pode ser configurado com arquivos
  de configuração na sintaxe XML ou YAML.'
sidebar_label: 'Arquivos de configuração'
sidebar_position: 50
slug: /operations/configuration-files
title: 'Arquivos de configuração'
doc_type: 'guide'
---

:::note
Perfis de configuração e arquivos de configuração baseados em XML não têm suporte no ClickHouse Cloud. Portanto, no ClickHouse Cloud, você não encontrará um arquivo `config.xml`. Em vez disso, use comandos SQL para gerenciar as configurações por meio de perfis de configuração.

Para mais detalhes, consulte [&quot;Como configurar settings&quot;](/pt-BR/manage/settings)
:::

O servidor ClickHouse pode ser configurado com arquivos de configuração na sintaxe XML ou YAML.
Na maioria dos tipos de instalação, o servidor ClickHouse é executado com `/etc/clickhouse-server/config.xml` como arquivo de configuração padrão, mas também é possível especificar manualmente o local do arquivo de configuração na inicialização do servidor usando a opção de linha de comando `--config-file` ou `-C`.
Arquivos de configuração adicionais podem ser colocados no diretório `config.d/`, relativo ao arquivo de configuração principal, por exemplo no diretório `/etc/clickhouse-server/config.d/`.
Os arquivos nesse diretório e a configuração principal são mesclados em uma etapa de pré-processamento antes de a configuração ser aplicada no servidor ClickHouse.
Os arquivos de configuração são mesclados em ordem alfabética.
Para simplificar as atualizações e melhorar a modularização, uma prática recomendada é manter o arquivo `config.xml` padrão sem modificações e colocar personalizações adicionais em `config.d/`.
A configuração do ClickHouse Keeper fica em `/etc/clickhouse-keeper/keeper_config.xml`.
Da mesma forma, arquivos de configuração adicionais para o Keeper precisam ser colocados em `/etc/clickhouse-keeper/keeper_config.d/`.

É possível misturar arquivos de configuração XML e YAML; por exemplo, você pode ter um arquivo de configuração principal `config.xml` e arquivos de configuração adicionais `config.d/network.xml`, `config.d/timezone.yaml` e `config.d/keeper.yaml`.
Não há suporte para misturar XML e YAML em um único arquivo de configuração.
Arquivos de configuração XML devem usar `<clickhouse>...</clickhouse>` como tag de nível superior.
Em arquivos de configuração YAML, `clickhouse:` é opcional; se estiver ausente, o parser o insere automaticamente.

<div id="merging">
  ## Mesclagem de configurações
</div>

Dois arquivos de configuração (geralmente o arquivo de configuração principal e outro arquivo de configuração em `config.d/`) são mesclados da seguinte forma:

* Se um nó (ou seja, um caminho que leva a um elemento) aparecer nos dois arquivos e não tiver os atributos `replace` ou `remove`, ele será incluído no arquivo de configuração mesclado, e os nós filhos de ambos também serão incluídos e mesclados recursivamente.
* Se um dos dois nós contiver o atributo `replace`, ele será incluído no arquivo de configuração mesclado, mas somente os nós filhos do nó com o atributo `replace` serão incluídos.
* Se um dos dois nós contiver o atributo `remove`, o nó não será incluído no arquivo de configuração mesclado (se já existir, será excluído).

Por exemplo, dados dois arquivos de configuração:

```xml title="config.xml"
<clickhouse>
    <config_a>
        <setting_1>1</setting_1>
    </config_a>
    <config_b>
        <setting_2>2</setting_2>
    </config_b>
    <config_c>
        <setting_3>3</setting_3>
    </config_c>
</clickhouse>
```

e

```xml title="config.d/other_config.xml"
<clickhouse>
    <config_a>
        <setting_4>4</setting_4>
    </config_a>
    <config_b replace="replace">
        <setting_5>5</setting_5>
    </config_b>
    <config_c remove="remove">
        <setting_6>6</setting_6>
    </config_c>
</clickhouse>
```

O arquivo de configuração resultante da mesclagem será:

```xml
<clickhouse>
    <config_a>
        <setting_1>1</setting_1>
        <setting_4>4</setting_4>
    </config_a>
    <config_b>
        <setting_5>5</setting_5>
    </config_b>
</clickhouse>
```

<div id="from_env_zk">
  ### Substituição por variáveis de ambiente e nós do ZooKeeper
</div>

Para especificar que o valor de um elemento deve ser substituído pelo valor de uma variável de ambiente, você pode usar o atributo `from_env`.

Por exemplo, com a variável de ambiente `$MAX_QUERY_SIZE = 150000`:

```xml
<clickhouse>
    <profiles>
        <default>
            <max_query_size from_env="MAX_QUERY_SIZE"/>
        </default>
    </profiles>
</clickhouse>
```

A configuração resultante será:

```xml
<clickhouse>
    <profiles>
        <default>
            <max_query_size>150000</max_query_size>
        </default>
    </profiles>
</clickhouse>
```

Isso também é possível usando `from_zk` (nó do ZooKeeper):

```xml
<clickhouse>
    <postgresql_port from_zk="/zk_configs/postgresql_port"/>
</clickhouse>
```

```shell
# clickhouse-keeper-client
/ :) touch /zk_configs
/ :) create /zk_configs/postgresql_port "9005"
/ :) get /zk_configs/postgresql_port
9005
```

O resultado é a seguinte configuração:

```xml
<clickhouse>
    <postgresql_port>9005</postgresql_port>
</clickhouse>
```

<div id="default-values">
  #### Valores padrão
</div>

Um elemento com os atributos `from_env` ou `from_zk` também pode ter o atributo `replace="1"` (este último deve aparecer antes de `from_env`/`from_zk`).
Nesse caso, o elemento pode definir um valor padrão.
O elemento assume o valor da variável de ambiente ou do nó do ZooKeeper, se estiver definido; caso contrário, assume o valor padrão.

O exemplo anterior é repetido, mas supondo que `MAX_QUERY_SIZE` não esteja definido:

```xml
<clickhouse>
    <profiles>
        <default>
            <max_query_size replace="1" from_env="MAX_QUERY_SIZE">150000</max_query_size>
        </default>
    </profiles>
</clickhouse>
```

Gerando a configuração:

```xml
<clickhouse>
    <profiles>
        <default>
            <max_query_size>150000</max_query_size>
        </default>
    </profiles>
</clickhouse>
```

<div id="substitution-with-file-content">
  ## Substituição com conteúdo de arquivo
</div>

Também é possível substituir partes da configuração pelo conteúdo de arquivos. Isso pode ser feito de duas maneiras:

* *Substituindo valores*: se um elemento tiver o atributo `incl`, seu valor será substituído pelo conteúdo do arquivo referenciado. Por padrão, o caminho para o arquivo com substituições é `/etc/metrika.xml`. Isso pode ser alterado no elemento [`include_from`](../operations/server-configuration-parameters/settings.md#include_from) da configuração do servidor. Os valores de substituição são especificados em elementos `/clickhouse/substitution_name` nesse arquivo. Se uma substituição especificada em `incl` não existir, ela será registrada no log. Para evitar que o ClickHouse registre substituições ausentes, especifique o atributo `optional="true"` (por exemplo, configurações de [macros](../operations/server-configuration-parameters/settings.md#macros)).
* *Substituindo elementos*: se você quiser substituir o elemento inteiro por uma substituição, use `include` como nome do elemento. O nome do elemento `include` pode ser combinado com o atributo `from_zk = "/path/to/node"`. Nesse caso, o valor do elemento será substituído pelo conteúdo do nó do ZooKeeper em `/path/to/node`. Isso também funciona quando você armazena uma subárvore XML inteira como um nó no ZooKeeper; ela será inserida integralmente no elemento de origem.

Um exemplo disso é mostrado abaixo:

```xml
<clickhouse>
    <!-- Appends XML subtree found at `/profiles-in-zookeeper` ZK path to `<profiles>` element. -->
    <profiles from_zk="/profiles-in-zookeeper" />

    <users>
        <!-- Replaces `include` element with the subtree found at `/users-in-zookeeper` ZK path. -->
        <include from_zk="/users-in-zookeeper" />
        <include from_zk="/other-users-in-zookeeper" />
    </users>
</clickhouse>
```

Se você quiser mesclar o conteúdo da substituição com a configuração existente, em vez de anexá-lo, pode usar o atributo `merge="true"`. Por exemplo: `<include from_zk="/some_path" merge="true">`. Nesse caso, a configuração existente será mesclada com o conteúdo da substituição, e as definições de configuração existentes serão substituídas pelos valores da substituição.

<div id="encryption">
  ## Criptografando e ocultando a configuração
</div>

Você pode usar criptografia simétrica para criptografar um elemento de configuração, por exemplo, uma senha em texto simples ou uma chave privada.
Para fazer isso, primeiro configure o [codec de criptografia](../sql-reference/statements/create/table.md#encryption-codecs) e, em seguida, adicione o atributo `encrypted_by`, com o nome do codec de criptografia como valor, ao elemento a ser criptografado.

Diferentemente dos atributos `from_zk`, `from_env` e `incl`, ou do elemento `include`, nenhuma substituição (isto é, a descriptografia do valor criptografado) é realizada no arquivo pré-processado.
A descriptografia acontece apenas em tempo de execução, no processo do servidor.

Por exemplo:

```xml
<clickhouse>

    <encryption_codecs>
        <aes_128_gcm_siv>
            <key_hex>00112233445566778899aabbccddeeff</key_hex>
        </aes_128_gcm_siv>
    </encryption_codecs>

    <interserver_http_credentials>
        <user>admin</user>
        <password encrypted_by="AES_128_GCM_SIV">961F000000040000000000EEDDEF4F453CFE6457C4234BD7C09258BD651D85</password>
    </interserver_http_credentials>

</clickhouse>
```

Os atributos [`from_env`](#from_env_zk) e [`from_zk`](#from_env_zk) também podem ser aplicados a `encryption_codecs`:

```xml
<clickhouse>

    <encryption_codecs>
        <aes_128_gcm_siv>
            <key_hex from_env="CLICKHOUSE_KEY_HEX"/>
        </aes_128_gcm_siv>
    </encryption_codecs>

    <interserver_http_credentials>
        <user>admin</user>
        <password encrypted_by="AES_128_GCM_SIV">961F000000040000000000EEDDEF4F453CFE6457C4234BD7C09258BD651D85</password>
    </interserver_http_credentials>

</clickhouse>
```

```xml
<clickhouse>

    <encryption_codecs>
        <aes_128_gcm_siv>
            <key_hex from_zk="/clickhouse/aes128_key_hex"/>
        </aes_128_gcm_siv>
    </encryption_codecs>

    <interserver_http_credentials>
        <user>admin</user>
        <password encrypted_by="AES_128_GCM_SIV">961F000000040000000000EEDDEF4F453CFE6457C4234BD7C09258BD651D85</password>
    </interserver_http_credentials>

</clickhouse>
```

Chaves de criptografia e valores criptografados podem ser definidos em qualquer um dos arquivos de configuração.

Um exemplo de `config.xml` é o seguinte:

```xml
<clickhouse>

    <encryption_codecs>
        <aes_128_gcm_siv>
            <key_hex from_zk="/clickhouse/aes128_key_hex"/>
        </aes_128_gcm_siv>
    </encryption_codecs>

</clickhouse>
```

Um exemplo de `users.xml` é fornecido a seguir:

```xml
<clickhouse>

    <users>
        <test_user>
            <password encrypted_by="AES_128_GCM_SIV">96280000000D000000000030D4632962295D46C6FA4ABF007CCEC9C1D0E19DA5AF719C1D9A46C446</password>
            <profile>default</profile>
        </test_user>
    </users>

</clickhouse>
```

Para criptografar um valor, você pode usar o programa (de exemplo) `encrypt_decrypt`:

```bash
./encrypt_decrypt /etc/clickhouse-server/config.xml -e AES_128_GCM_SIV abcd
```

```text
961F000000040000000000EEDDEF4F453CFE6457C4234BD7C09258BD651D85
```

Mesmo com elementos de configuração criptografados, eles ainda aparecem no arquivo de configuração pré-processado.
Se isso for um problema para a sua implantação do ClickHouse, há duas alternativas: definir as permissões do arquivo pré-processado como 600 ou usar o atributo `hide_in_preprocessed`.

Por exemplo:

```xml
<clickhouse>

    <interserver_http_credentials hide_in_preprocessed="true">
        <user>admin</user>
        <password>secret</password>
    </interserver_http_credentials>

</clickhouse>
```

<div id="user-settings">
  ## Configuração do usuário
</div>

O arquivo `config.xml` pode especificar uma configuração separada com configuração do usuário, perfis e cotas. O caminho relativo para essa configuração é definido no elemento `users_config`. Por padrão, ele é `users.xml`. Se `users_config` for omitido, a configuração do usuário, os perfis e as cotas serão especificados diretamente em `config.xml`.

A configuração do usuário pode ser dividida em arquivos separados, de forma semelhante a `config.xml` e `config.d/`.
O nome do diretório é definido como a configuração `users_config` sem o sufixo `.xml`, concatenada com `.d`.
O diretório `users.d` é usado por padrão, já que `users_config` tem como padrão `users.xml`.

Observe que os arquivos de configuração são primeiro [mesclados](#merging), levando em conta as configurações, e os includes são processados depois disso.

<div id="example">
  ## Exemplo em XML
</div>

Por exemplo, você pode ter um arquivo de configuração separado para cada usuário assim:

```bash
$ cat /etc/clickhouse-server/users.d/alice.xml
```

```xml
<clickhouse>
    <users>
      <alice>
          <profile>analytics</profile>
            <networks>
                  <ip>::/0</ip>
            </networks>
          <password_sha256_hex>...</password_sha256_hex>
          <quota>analytics</quota>
      </alice>
    </users>
</clickhouse>
```

<div id="example-1">
  ## Exemplos de YAML
</div>

Aqui você pode ver a configuração padrão escrita em YAML: [`config.yaml.example`](https://github.com/ClickHouse/ClickHouse/blob/master/programs/server/config.yaml.example).

Há algumas diferenças entre os formatos YAML e XML no que diz respeito às configurações do ClickHouse.
A seguir, são apresentadas dicas para escrever configurações no formato YAML.

Uma tag XML com um valor de texto é representada por um par chave-valor no YAML

```yaml
key: value
```

XML correspondente:

```xml
<key>value</key>
```

Um nó XML aninhado é representado por um mapeamento YAML:

```yaml
map_key:
  key1: val1
  key2: val2
  key3: val3
```

XML correspondente:

```xml
<map_key>
    <key1>val1</key1>
    <key2>val2</key2>
    <key3>val3</key3>
</map_key>
```

Para criar a mesma tag XML várias vezes, use uma sequência YAML:

```yaml
seq_key:
  - val1
  - val2
  - key1: val3
  - map:
      key2: val4
      key3: val5
```

XML correspondente:

```xml
<seq_key>val1</seq_key>
<seq_key>val2</seq_key>
<seq_key>
    <key1>val3</key1>
</seq_key>
<seq_key>
    <map>
        <key2>val4</key2>
        <key3>val5</key3>
    </map>
</seq_key>
```

Para informar um atributo XML, você pode usar uma chave de atributo com o prefixo `@`. Observe que `@` é reservado pelo padrão YAML e, por isso, deve ser colocado entre aspas duplas:

```yaml
map:
  "@attr1": value1
  "@attr2": value2
  key: 123
```

XML correspondente:

```xml
<map attr1="value1" attr2="value2">
    <key>123</key>
</map>
```

Também é possível usar atributos em uma sequência YAML:

```yaml
seq:
  - "@attr1": value1
  - "@attr2": value2
  - 123
  - abc
```

XML correspondente:

```xml
<seq attr1="value1" attr2="value2">123</seq>
<seq attr1="value1" attr2="value2">abc</seq>
```

A sintaxe mencionada anteriormente não permite representar, em YAML, nós de texto XML com atributos XML. Esse caso especial pode ser tratado usando uma
chave de atributo `#text`:

```yaml
map_key:
  "@attr1": value1
  "#text": value2
```

XML correspondente:

```xml
<map_key attr1="value1">value2</map>
```

<div id="implementation-details">
  ## Detalhes de implementação
</div>

Para cada arquivo de configuração, o servidor também gera arquivos `file-preprocessed.xml` na inicialização. Esses arquivos contêm todas as substituições e sobreposições já concluídas e se destinam apenas a fins informativos. Se tiverem sido usadas substituições do ZooKeeper nos arquivos de configuração, mas o ZooKeeper não estiver disponível na inicialização do servidor, o servidor carregará a configuração a partir do arquivo pré-processado.

O servidor monitora alterações nos arquivos de configuração, bem como nos arquivos e nós do ZooKeeper usados ao realizar substituições e sobreposições, e recarrega dinamicamente as configurações de usuários e clusters. Isso significa que você pode modificar o cluster, os usuários e suas configurações sem reiniciar o servidor.