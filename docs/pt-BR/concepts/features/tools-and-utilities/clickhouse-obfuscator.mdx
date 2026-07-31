---
description: 'Documentação do ClickHouse Obfuscator'
slug: /operations/utilities/clickhouse-obfuscator
title: 'clickhouse-obfuscator'
doc_type: 'reference'
---

Uma ferramenta simples para ofuscação de dados de tabelas.

Ela lê uma tabela de entrada e produz uma tabela de saída que preserva algumas propriedades da entrada, mas contém dados diferentes.
Ela permite publicar dados de produção quase reais para uso em benchmarks.

Ela foi projetada para preservar as seguintes propriedades dos dados:

* cardinalidades dos valores (número de valores distintos) para cada coluna e cada tupla de colunas;

* cardinalidades condicionais: número de valores distintos de uma coluna sob a condição do valor de outra coluna;

* distribuições de probabilidade do valor absoluto de inteiros; do sinal de inteiros com sinal; do expoente e do sinal de floats;

* distribuições de probabilidade do comprimento de strings;

* probabilidade de valores zero em números; strings e arrays vazios; `NULL`s;

* razão de compressão dos dados quando comprimidos com LZ77 e a família de codecs de entropia;

* continuidade (magnitude da diferença) dos valores de tempo ao longo da tabela; continuidade de valores de ponto flutuante;

* componente de data dos valores `DateTime`;

* validade UTF-8 de valores de string;

* os valores de string parecem naturais.

A maioria das propriedades acima é útil para testes de desempenho:

ler dados, filtrar, agregar e ordenar funcionará quase na mesma velocidade
que nos dados originais devido às cardinalidades, magnitudes, razões de compressão etc. preservadas.

Ela funciona de forma determinística: você define um valor de seed, e a transformação é determinada pelos dados de entrada e pela seed.
Algumas transformações são um para um e podem ser revertidas, portanto você precisa ter uma seed grande e mantê-la em segredo.

Ela usa algumas primitivas criptográficas para transformar os dados, mas, do ponto de vista criptográfico, não faz isso corretamente. Por isso, você não deve considerar o resultado seguro, a menos que tenha outro motivo. O resultado pode preservar alguns dados que você não quer publicar.

Ela sempre deixa exatamente como nos dados de origem os números 0, 1, -1, datas, comprimentos de arrays e flags nulas.
Por exemplo, você tem uma coluna `IsMobile` em sua tabela com valores 0 e 1. Nos dados transformados, ela terá o mesmo valor.

Assim, o usuário poderá calcular a proporção exata de tráfego móvel.

Vamos dar outro exemplo. Quando você tem alguns dados privados em sua tabela, como o email do usuário, e não quer publicar nenhum endereço de email.
Se sua tabela for grande o suficiente, contiver vários emails diferentes e nenhum email tiver uma frequência muito maior que os demais, ela anonimizará todos os dados. Mas, se você tiver um pequeno número de valores diferentes em uma coluna, ela poderá reproduzir alguns deles.
Você deve analisar como o algoritmo desta ferramenta funciona e ajustar seus parâmetros de linha de comando.

Esta ferramenta só funciona bem com pelo menos uma quantidade moderada de dados (pelo menos milhares de linhas).