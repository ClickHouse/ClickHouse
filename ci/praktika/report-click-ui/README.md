# ClickHouse CI Results Viewer

A static web page built with React and Click UI that displays ClickHouse CI test results from S3 JSON files in a table format.

## What This Is

This is a standalone web application that:
- Fetches CI test results from ClickHouse S3 buckets (JSON format)
- Displays results in a table with: Status, Name, Duration, Start Time
- Supports URL parameters to load different JSON files
- Works as a pure static site (no backend needed) - can be uploaded to S3
- Includes dark/light theme toggle

## Project Structure

```
ci/praktika/report-click-ui/
├── src/
│   ├── App.tsx          # Main component with table logic
│   ├── main.tsx         # React entry point
│   └── ...
├── dist/                # Built static files (ready for S3 upload)
├── vite.config.ts       # Vite config with CORS proxy for development
└── package.json
```

## Technologies Used

- **React 18.3.1** (Click UI requires React 18, not 19)
- **Click UI** (@clickhouse/click-ui) - ClickHouse's official component library
- **Vite** - Build tool and dev server
- **TypeScript**
- **styled-components** (required by Click UI)

## Setup

### Prerequisites
- Node.js >= 18

### Installation

```bash
cd ci/praktika/report-click-ui
npm install
```

### Important Dependencies

The project uses these peer dependencies for Click UI:
- `react@18.3.1` and `react-dom@18.3.1` (NOT React 19!)
- `styled-components` (Click UI uses this for styling, no separate CSS needed)
- `dayjs`

## Development

### Start Dev Server

```bash
npm run dev
```

Opens at http://localhost:5173

**Note:** The dev server includes a CORS proxy (`/s3-proxy/*`) that routes S3 requests through localhost to avoid CORS errors during development.

### Build for Production

```bash
npm run build
```

Output goes to `dist/` directory - these are static files ready for S3 upload.

### Preview Production Build

```bash
npm run preview
```

Opens at http://localhost:4173

## How It Works

### URL Parameters

The page supports two modes for loading JSON files:

#### Mode 1: PR or REF with SHA Parameters (Recommended)

Pass either `PR` (for pull requests) or `REF` (for branches) along with `SHA`:

**For Pull Requests:**
```
http://localhost:5173/?PR=96792&SHA=dd4e76d16912546d3cdbcfb8c14b076b6ad28ee6
```

Constructs: `{baseUrl}/PRs/{PR}/{SHA}/result_pr.json`

**For Branches:**
```
http://localhost:5173/?REF=master&SHA=dd4e76d16912546d3cdbcfb8c14b076b6ad28ee6
```

Constructs: `{baseUrl}/REFs/{REF}/{SHA}/result_master.json`

Where `baseUrl` is:
- **Development:** `https://s3.amazonaws.com/clickhouse-test-reports`
- **Production:** `{origin}/clickhouse-test-reports`

Either `PR` or `REF` must be provided (not both), along with `SHA`.

**With dynamic filename and metadata (using name_* parameters):**

Works with both `PR` and `REF`:

```
# PR with custom metadata
http://localhost:5173/?PR=96792&SHA=dd4e76d&name_0=PR&name_1=CustomValue&name_2=AnotherValue

# Branch with custom metadata
http://localhost:5173/?REF=master&SHA=dd4e76d&name_0=master&name_1=Nightly&name_2=Production
```

- `name_0` defines the filename as `result_{name_0}.json` (e.g., `name_0=PR` → `result_pr.json`)
- `name_1`, `name_2`, ... `name_N` are displayed on the page as metadata
- All name parameters are shown in the UI above the table

**Default filenames if `name_0` is not provided:**
- For `PR`: defaults to `result_pr.json`
- For `REF`: defaults to `result_{ref}.json` (e.g., `result_master.json` for `REF=master`)

**Examples:**
```
# Pull request with default filename
?PR=96792&SHA=dd4e76d&name_0=PR

# Master branch with default filename
?REF=master&SHA=dd4e76d&name_0=master

# Development branch
?REF=dev&SHA=abc123&name_0=dev

# PR with additional metadata
?PR=96792&SHA=dd4e76d&name_0=PR&name_1=CI-Run-123&name_2=Production

# Branch with additional metadata
?REF=master&SHA=dd4e76d&name_0=master&name_1=Nightly&name_2=Staging
```

#### Mode 2: Direct URL

Pass the full URL directly:

```
http://localhost:5173/?url=https://s3.amazonaws.com/clickhouse-test-reports/PRs/12345/abc/result_pr.json
```

#### Default (no parameters)

```
http://localhost:5173/
```

Uses hardcoded default: `https://s3.amazonaws.com/clickhouse-test-reports/PRs/96792/dd4e76d16912546d3cdbcfb8c14b076b6ad28ee6/result_pr.json`

### CORS Handling

**Development (localhost):**
- Uses proxy in `vite.config.ts`
- S3 URLs are rewritten: `https://s3.amazonaws.com/*` → `/s3-proxy/*`
- Proxy forwards to S3, bypassing CORS

**Production (S3):**
- Uses direct S3 URLs
- No CORS issues when hosted on S3 (same-origin requests)
- Code automatically detects `import.meta.env.DEV` to switch between proxy/direct URLs

### Data Format

The app expects JSON in this format:

```json
{
  "name": "PR",
  "status": "success",
  "start_time": "2024-01-01T12:00:00",
  "duration": 1234.5,
  "results": [
    {
      "name": "Test Name",
      "status": "success",
      "start_time": "2024-01-01T12:00:00",
      "duration": 123.4,
      "info": "Additional info"
    }
  ]
}
```

The `results` array is displayed in the table.

### Table Columns

1. **Status** - Test result status (success, failed, skipped, etc.)
2. **Name** - Test name
3. **Duration** - Formatted as "Xm Ys" (e.g., "2m 34s")
4. **Start Time** - Formatted as locale date/time string

## Deployment to S3

### Upload Static Files

```bash
cd ci/praktika/report-click-ui
aws s3 sync dist/ s3://your-bucket-name/ --acl public-read
```

### S3 Bucket Configuration

1. **Enable Static Website Hosting:**
   - Bucket Settings → Properties → Static website hosting
   - Index document: `index.html`

2. **Set Permissions:**
   - Make objects public (or use CloudFront with OAI)

3. **Optional: Configure CORS** (if accessing from other domains):
   ```json
   [
     {
       "AllowedHeaders": ["*"],
       "AllowedMethods": ["GET"],
       "AllowedOrigins": ["*"],
       "ExposeHeaders": []
     }
   ]
   ```

### Access Your Page

```
https://your-bucket.s3.amazonaws.com/index.html
https://your-bucket.s3.amazonaws.com/index.html?url=https://s3.amazonaws.com/.../result.json
```

## Code Overview

### Key Components

**App.tsx** contains:
- State management: `data`, `loading`, `error`, `theme`, `nameParams`
- `useEffect` hook to fetch JSON on mount
- URL parameter parsing:
  - `PR` or `REF` with `SHA` for URL construction
  - Automatic path selection: `PRs/` for PR, `REFs/` for branches
  - `name_0`, `name_1`, ... `name_N` for dynamic filename and metadata
  - Direct `url` parameter as fallback
- CORS proxy logic for dev vs production
- Table data formatting (duration, timestamp)
- Metadata display from name parameters
- Click UI components: `ClickUIProvider`, `Container`, `Title`, `Text`, `Switch`, `Table`

**main.tsx:**
- React entry point
- Imports App component

**vite.config.ts:**
- Proxy configuration for `/s3-proxy/*` → `https://s3.amazonaws.com/*`

## Troubleshooting

### CORS Errors

**Problem:** `Access to fetch at 'https://s3.amazonaws.com/...' has been blocked by CORS policy`

**Solution:**
- In development: Make sure dev server is running (`npm run dev`), not preview
- In production: Deploy to S3 so requests are same-origin

### React Version Conflicts

**Problem:** `Cannot read properties of undefined (reading 'ReactCurrentDispatcher')`

**Solution:**
- Click UI requires React 18, not React 19
- Check: `npm list react react-dom --depth=0`
- Fix: `npm install react@18.3.1 react-dom@18.3.1`

### Build Warnings

The build shows warnings about large chunks (>500KB). This is expected because Click UI and its dependencies are large. For production, consider:
- Code splitting with dynamic imports
- Tree shaking (enabled by default in Vite)

## Future Enhancements

Ideas for further development:

1. **Colored Status Badges**
   - Use Click UI `Badge` component
   - Green for success, red for failed, gray for skipped

2. **Sortable Columns**
   - Use Table component's `onSort` prop
   - Sort by status, duration, name, etc.

3. **Filtering**
   - Add search input to filter by test name
   - Filter by status (show only failed tests)

4. **Nested Results**
   - Expandable rows to show sub-results
   - Some tests have nested `results` arrays

5. **Artifact Links**
   - Display `links` from JSON (logs, build artifacts)
   - Make clickable links in table

6. **Error Details**
   - Show `info` field for failed tests
   - Add tooltip or expandable details

7. **Custom Base Path**
   - Add `PATH` parameter to override base path structure
   - Example: `?PATH=custom/path&SHA=dd4e76d&name_0=custom`
   - Useful for non-standard S3 structures

8. **Loading Skeleton**
   - Better loading state with skeleton UI
   - Click UI might have skeleton components

9. **Pagination**
   - For results with many tests
   - Click UI Table supports pagination

10. **Export to CSV**
    - Download table data as CSV file

## Important Notes

- Always use React 18 (Click UI doesn't support React 19 yet)
- Click UI uses styled-components (no separate CSS import needed)
- The proxy in vite.config.ts is only for development
- Production build uses direct S3 URLs (no proxy)
- When hosted on S3, CORS shouldn't be an issue for same-bucket requests

## Resources

- [Click UI Documentation](https://clickhouse.design/click-ui)
- [Click UI GitHub](https://github.com/ClickHouse/click-ui)
- [Vite Documentation](https://vitejs.dev)
- [React 18 Docs](https://react.dev)

## Quick Reference

```bash
# Install dependencies
npm install

# Development (with CORS proxy)
npm run dev              # http://localhost:5173

# Build for production
npm run build

# Preview production build
npm run preview          # http://localhost:4173

# Deploy to S3
aws s3 sync dist/ s3://your-bucket/ --acl public-read

# Check React version
npm list react react-dom --depth=0
```

## Project History

This project was created to visualize ClickHouse CI test results from S3 JSON files. It started as a simple hello world with Click UI, then evolved into a table viewer with:
- Dynamic JSON loading
- URL parameter support
- CORS workarounds for development
- Production-ready static build for S3 deployment
