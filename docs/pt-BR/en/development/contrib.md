---
description: 'Página que descreve o uso de bibliotecas de terceiros no ClickHouse e como adicionar e manter
  bibliotecas de terceiros.'
sidebar_label: 'Bibliotecas de terceiros'
sidebar_position: 60
slug: /development/contrib
title: 'Bibliotecas de terceiros'
doc_type: 'reference'
---

O ClickHouse utiliza bibliotecas de terceiros para diferentes finalidades, por exemplo, para se conectar a outros bancos de dados, decodificar/codificar dados durante o carregamento/salvamento em disco ou implementar determinadas funções SQL especializadas.
Para não depender das bibliotecas disponíveis no sistema de destino, cada biblioteca de terceiros é importada como um submódulo Git para a árvore de código-fonte do ClickHouse e compilada e vinculada ao ClickHouse.
Uma lista das bibliotecas de terceiros e de suas licenças pode ser obtida com a seguinte consulta:

```sql
SELECT library_name, license_type, license_path FROM system.licenses ORDER BY library_name COLLATE 'en';
```

Note que as bibliotecas listadas são as que estão localizadas no diretório `contrib/` do repositório do ClickHouse.
Dependendo das opções de compilação, algumas dessas bibliotecas podem não ter sido compiladas e, como resultado, sua funcionalidade pode não estar disponível em tempo de execução.

[Exemplo](https://sql.clickhouse.com?query_id=478GCPU7LRTSZJBNY3EJT3)

<div id="adding-and-maintaining-third-party-libraries">
  ## Adicionando e mantendo bibliotecas de terceiros
</div>

Cada biblioteca de terceiros deve ficar em um diretório dedicado dentro do diretório `contrib/` do repositório do ClickHouse.
Evite simplesmente despejar cópias de código externo no diretório da biblioteca.
Em vez disso, crie um submódulo Git para extrair código de terceiros de um repositório upstream externo.

Todos os submódulos usados pelo ClickHouse estão listados no arquivo `.gitmodule`.

* Se a biblioteca puder ser usada como está (o caso padrão), você pode referenciar diretamente o repositório upstream.
* Se a biblioteca precisar de patches, crie um fork do repositório upstream na [organização ClickHouse no GitHub](https://github.com/ClickHouse).

Neste último caso, nosso objetivo é isolar ao máximo os patches personalizados dos commits upstream.
Para isso, crie uma branch com o prefixo `ClickHouse/` a partir da branch ou tag que você quer integrar, por exemplo `ClickHouse/2024_2` (para a branch `2024_2`) ou `ClickHouse/release/vX.Y.Z` (para a tag `release/vX.Y.Z`).
Evite acompanhar branches de desenvolvimento upstream como `master` / `main` / `dev` (ou seja, branches com prefixo `ClickHouse/master` / `ClickHouse/main` / `ClickHouse/dev` no repositório com fork).
Essas branches são alvos móveis, o que dificulta o versionamento adequado.
As &quot;branches com prefixo&quot; garantem que pulls do repositório upstream para o fork não afetem as branches personalizadas `ClickHouse/`.
Os submódulos em `contrib/` devem rastrear apenas branches `ClickHouse/` de repositórios de terceiros com fork.

Os patches só são aplicados sobre branches `ClickHouse/` de bibliotecas externas.

Há duas maneiras de fazer isso:

* se você quiser criar uma nova correção em uma branch com prefixo `ClickHouse/` no repositório com fork, por exemplo uma correção de sanitizer. Nesse caso, envie a correção como uma branch com prefixo `ClickHouse/`, por exemplo `ClickHouse/fix-sanitizer-disaster`. Em seguida, crie uma PR da nova branch para a branch de rastreamento personalizada, por exemplo `ClickHouse/2024_2 <-- ClickHouse/fix-sanitizer-disaster`, e faça o merge da PR.
* se você atualizar o submódulo e precisar reaplicar patches anteriores. Nesse caso, recriar PRs antigas é exagero. Em vez disso, simplesmente faça cherry-pick de commits antigos na nova branch `ClickHouse/` (correspondente à nova versão). Sinta-se à vontade para fazer squash dos commits de PRs que tinham vários commits. No melhor dos casos, já contribuímos os patches personalizados de volta ao upstream e podemos omitir esses patches na nova versão.

Depois que o submódulo for atualizado, atualize o submódulo no ClickHouse para apontar para o novo hash no fork.

Crie patches para bibliotecas de terceiros tendo em mente o repositório oficial e considere contribuir com o patch de volta para o repositório upstream.
Isso garante que outras pessoas também se beneficiem do patch e que ele não se torne um ônus de manutenção para a equipe da ClickHouse.