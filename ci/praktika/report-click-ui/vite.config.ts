import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  base: './', // Use relative paths for assets
  // Uncomment for local development with a symlinked click-ui:
  // resolve: {
  //   dedupe: ['react', 'react-dom', 'styled-components'],
  // },
  server: {
    proxy: {
      '/s3-proxy': {
        target: 'https://s3.amazonaws.com',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/s3-proxy/, ''),
      },
    },
  },
})
