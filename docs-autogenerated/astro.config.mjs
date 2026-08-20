import { fileURLToPath } from 'node:url';

import mdx from '@astrojs/mdx';
import { unified } from '@astrojs/markdown-remark';
import react from '@astrojs/react';
import { mintlify } from '@mintlify/astro';
import tailwindcss from '@tailwindcss/vite';
import { defineConfig } from 'astro/config';
import rehypeKatex from 'rehype-katex';
import remarkMath from 'remark-math';

import rehypeCodeBlocks from './src/markdown/code-blocks.mjs';
import rehypeCustomCodeHighlighter from './src/markdown/custom-code-highlighter.mjs';
import rewriteGeneratedCodeBlocks from './src/markdown/rewrite-generated-code-blocks.mjs';

export default defineConfig({
  site: 'https://clickhouse.com',
  output: 'static',
  publicDir: './src/generated/public',
  trailingSlash: 'ignore',
  markdown: {
    syntaxHighlight: false,
    processor: unified({
      remarkPlugins: [remarkMath],
      rehypePlugins: [rehypeKatex, rehypeCustomCodeHighlighter, rehypeCodeBlocks],
    }),
  },
  integrations: [
    mintlify({ docsDir: './src/generated/mintlify' }),
    rewriteGeneratedCodeBlocks(),
    react(),
    mdx(),
  ],
  vite: {
    plugins: [tailwindcss()],
    resolve: {
      alias: {
        '@components': fileURLToPath(new URL('./src/components', import.meta.url)),
        '@generated': fileURLToPath(new URL('./src/generated', import.meta.url)),
      },
    },
  },
});
