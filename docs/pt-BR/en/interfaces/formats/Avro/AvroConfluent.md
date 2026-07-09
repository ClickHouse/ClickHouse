---
alias: []
description: 'Documentação sobre o formato AvroConfluent'
input_format: true
keywords: ['AvroConfluent']
output_format: true
slug: /interfaces/formats/AvroConfluent
title: 'AvroConfluent'
doc_type: 'reference'
---

import DataTypesMatching from './_snippets/data-types-matching.md'

| Entrada | Saída | Alias |
| ------- | ----- | ----- |
| ✔       | ✔     |       |

<div id="description">
  ## Descrição
</div>

[Apache Avro](https://avro.apache.org/) é um formato de serialização orientado a linhas que usa codificação binária para um processamento eficiente de dados. O formato `AvroConfluent` oferece suporte à leitura e à gravação de mensagens codificadas em Avro usando o [Confluent Schema Registry](https://docs.confluent.io/current/schema-registry/index.html) (ou serviços compatíveis com a API).

Cada mensagem usa o formato wire da Confluent: um byte mágico (`0x00`), seguido por um schema ID de 4 bytes em big-endian e, em seguida, pelo dado binário Avro. Na leitura, o ClickHouse resolve o schema ID consultando o registro. Na gravação, o ClickHouse registra o schema derivado das colunas de saída e acrescenta o ID resultante no início de cada linha. Os schemas são armazenados em cache para desempenho ideal.

<a id="data-types-matching" />

<div id="data-type-mapping">
  ## Mapeamento de tipos de dados
</div>

<DataTypesMatching />

<div id="format-settings">
  ## Configurações de formato
</div>

[//]: # "OBSERVAÇÃO Estas configurações podem ser definidas em nível de sessão, mas isso não é comum, e dar muito destaque a isso na documentação pode confundir os usuários."

| Configuração                                     | Descrição                                                                                                                                                                              | Padrão |
| ------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------ |
| `input_format_avro_allow_missing_fields`         | Se deve usar um valor padrão em vez de gerar um erro quando um campo não for encontrado no schema.                                                                                     | `0`    |
| `input_format_avro_null_as_default`              | Se deve usar um valor padrão em vez de gerar um erro ao inserir um valor `null` em uma coluna que não aceita nulos.                                                                    | `0`    |
| `format_avro_schema_registry_url`                | A URL do Confluent Schema Registry. Para autenticação básica, credenciais codificadas em URL podem ser incluídas diretamente no path da URL.                                           |        |
| `format_avro_schema_registry_connection_timeout` | Timeout de conexão, em segundos, para o cliente HTTP do Schema Registry (usado tanto para buscar o schema quanto para registrá-lo). Deve ser maior que 0 e menor que 600 (10 minutos). | `1`    |
| `format_avro_schema_registry_send_timeout`       | Timeout de envio, em segundos, para o cliente HTTP do Schema Registry. Deve ser maior que 0 e menor que 600 (10 minutos).                                                              | `1`    |
| `format_avro_schema_registry_receive_timeout`    | Timeout de recebimento, em segundos, para o cliente HTTP do Schema Registry. Deve ser maior que 0 e menor que 600 (10 minutos).                                                        | `1`    |
| `output_format_avro_confluent_subject`           | Para saída: o nome do subject sob o qual o schema é registrado no Schema Registry. Obrigatório na gravação.                                                                            |        |
| `output_format_avro_string_column_pattern`       | Para saída: regexp de colunas String a serem serializadas como Avro `string` (o padrão é `bytes`).                                                                                     |        |

<div id="examples">
  ## Exemplos
</div>

<div id="reading-from-kafka">
  ### Lendo do Kafka
</div>

Para ler um tópico do Kafka codificado em Avro usando o [motor de tabela Kafka](/pt-BR/engines/table-engines/integrations/kafka.md), use a configuração `format_avro_schema_registry_url` para informar a URL do registro de schemas.

```sql
CREATE TABLE topic1_stream
(
    field1 String,
    field2 String
)
ENGINE = Kafka()
SETTINGS
kafka_broker_list = 'kafka-broker',
kafka_topic_list = 'topic1',
kafka_group_name = 'group1',
kafka_format = 'AvroConfluent',
format_avro_schema_registry_url = 'http://schema-registry-url';

SELECT * FROM topic1_stream;
```

<div id="writing-to-kafka">
  ### Gravar no Kafka
</div>

Para gravar mensagens AvroConfluent em um tópico do Kafka, defina a URL do registro de schemas e o nome do subject. O schema é registrado automaticamente no registro na primeira escrita.

```sql
CREATE TABLE topic1_sink
(
    field1 String,
    field2 String
)
ENGINE = Kafka()
SETTINGS
kafka_broker_list = 'kafka-broker',
kafka_topic_list = 'topic1',
kafka_format = 'AvroConfluent',
format_avro_schema_registry_url = 'http://schema-registry-url',
output_format_avro_confluent_subject = 'topic1-value';

INSERT INTO topic1_sink VALUES ('hello', 'world');
```

<div id="using-basic-authentication">
  #### Usando autenticação básica
</div>

Se o seu registro de esquemas exigir autenticação básica (por exemplo, se você estiver usando o Confluent Cloud), você poderá informar credenciais codificadas em URL na configuração `format_avro_schema_registry_url`.

```sql
CREATE TABLE topic1_stream
(
    field1 String,
    field2 String
)
ENGINE = Kafka()
SETTINGS
kafka_broker_list = 'kafka-broker',
kafka_topic_list = 'topic1',
kafka_group_name = 'group1',
kafka_format = 'AvroConfluent',
format_avro_schema_registry_url = 'https://<username>:<password>@schema-registry-url';
```

<div id="troubleshooting">
  ## Solução de problemas
</div>

Para monitorar o progresso da ingestão e depurar erros no consumidor do Kafka, é possível consultar a [tabela de sistema `system.kafka_consumers`](../../../operations/system-tables/kafka_consumers.md). Se a sua implantação tiver várias réplicas (por exemplo, ClickHouse Cloud), você deverá usar a [função de tabela `clusterAllReplicas`](../../../sql-reference/table-functions/cluster.md).

```sql
SELECT * FROM clusterAllReplicas('default',system.kafka_consumers)
ORDER BY assignments.partition_id ASC;
```

Se você encontrar problemas de resolução de schema, poderá usar [kafkacat](https://github.com/edenhill/kafkacat) com [clickhouse-local](/pt-BR/operations/utilities/clickhouse-local.md) para diagnosticar o problema:

```bash
$ kafkacat -b kafka-broker  -C -t topic1 -o beginning -f '%s' -c 3 | clickhouse-local   --input-format AvroConfluent --format_avro_schema_registry_url 'http://schema-registry' -S "field1 Int64, field2 String"  -q 'select *  from table'
1 a
2 b
3 c
```