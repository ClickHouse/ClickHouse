---
description: 'Dicionários geobase nativos no ClickHouse'
sidebar_label: 'Dicionários embutidos'
sidebar_position: 6
slug: /sql-reference/statements/create/dictionary/embedded
title: 'Dicionários embutidos (geobase)'
doc_type: 'referência'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';

<SelfManaged />

O ClickHouse contém um recurso integrado para trabalhar com uma geobase.

Isso permite:

* Usar o ID de uma região para obter seu nome no idioma desejado.
* Usar o ID de uma região para obter o ID de uma cidade, área, distrito federal, país ou continente.
* Verificar se uma região faz parte de outra região.
* Obter uma cadeia de regiões pai.

Todas as funções oferecem suporte a &quot;translocality&quot;, ou seja, a capacidade de usar simultaneamente diferentes perspectivas sobre a vinculação de regiões. Para mais informações, consulte a seção &quot;Funções para trabalhar com dicionários de web analytics&quot;.

Os dicionários internos ficam desabilitados no pacote padrão.
Para habilitá-los, descomente os parâmetros `path_to_regions_hierarchy_file` e `path_to_regions_names_files` no arquivo de configuração do servidor.

A geobase é carregada a partir de arquivos de texto.

Coloque os arquivos `regions_hierarchy*.txt` no diretório `path_to_regions_hierarchy_file`. Esse parâmetro de configuração deve conter o caminho para o arquivo `regions_hierarchy.txt` (a hierarquia regional padrão), e os outros arquivos (`regions_hierarchy_ua.txt`) devem estar no mesmo diretório.

Coloque os arquivos `regions_names_*.txt` no diretório `path_to_regions_names_files`.

Você também pode criar esses arquivos por conta própria. O formato do arquivo é o seguinte:

`regions_hierarchy*.txt`: TabSeparated (sem cabeçalho), colunas:

* ID da região (`UInt32`)
* ID da região pai (`UInt32`)
* tipo da região (`UInt8`): 1 - continente, 3 - país, 4 - distrito federal, 5 - região, 6 - cidade; os demais tipos não têm valores
* população (`UInt32`) — coluna opcional

`regions_names_*.txt`: TabSeparated (sem cabeçalho), colunas:

* ID da região (`UInt32`)
* nome da região (`String`) — Não pode conter tabulações nem quebras de linha, mesmo escapadas.

Um array plano é usado para armazenamento em RAM. Por esse motivo, os IDs não devem ser maiores que um milhão.

Os dicionários podem ser atualizados sem reiniciar o servidor. No entanto, o conjunto de dicionários disponíveis não é atualizado.
Nas atualizações, são verificados os horários de modificação dos arquivos. Se um arquivo tiver sido alterado, o dicionário será atualizado.
O intervalo de verificação de alterações é configurado no parâmetro `builtin_dictionaries_reload_interval`.
As atualizações de dicionário (exceto o carregamento no primeiro uso) não bloqueiam consultas. Durante as atualizações, as consultas usam as versões antigas dos dicionários. Se ocorrer um erro durante uma atualização, o erro será gravado no log do servidor, e as consultas continuarão usando a versão antiga dos dicionários.

Recomendamos atualizar periodicamente os dicionários com a geobase. Durante uma atualização, gere novos arquivos e grave-os em um local separado. Quando tudo estiver pronto, renomeie-os para substituir os arquivos usados pelo servidor.

Também há funções para trabalhar com identificadores de SO e mecanismos de busca, mas elas não devem ser usadas.