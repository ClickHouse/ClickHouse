export const PreviewFeatures = () =>
{
  const previewsEndpoint = 'https://console.clickhouse.cloud/previews.json';

  function getConsoleUrl(previewId)
  {
    const url = new URL('https://console.clickhouse.cloud/');
    url.searchParams.set('loc', `docs-self-service-previews-${previewId}`);
    return url.toString();
  }

  function getPreviewTypeLabel(type)
  {
    return type === 'public-preview' ? 'Public preview' : 'Private preview';
  }

  function getActionLabel(type)
  {
    return type === 'public-preview' ? 'Sign in to enable' : 'Sign in to request access';
  }

  function isPreview(preview)
  {
    return (
      typeof preview?.id === 'string' &&
      typeof preview?.name === 'string' &&
      typeof preview?.description === 'string' &&
      (preview.type === 'public-preview' || preview.type === 'private-preview') &&
      (preview.docsUrl === null || typeof preview.docsUrl === 'string')
    );
  }

  function usePreviewFeatures()
  {
    const cached = typeof window !== 'undefined' ? window.__chPreviewFeaturesCache : undefined;
    const [previews, setPreviews] = useState(cached || []);
    const [loading, setLoading] = useState(cached === undefined);
    const [error, setError] = useState(null);

    useEffect(() =>
    {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 8000);

      async function fetchPreviews()
      {
        try
        {
          const response = await fetch(previewsEndpoint, {
            headers: { Accept: 'application/json' },
            signal: controller.signal,
          });

          if (!response.ok)
            throw new Error(`Preview features request failed with HTTP ${response.status}`);

          const data = await response.json();
          if (data.version !== 1 || !Array.isArray(data.previews) || !data.previews.every(isPreview))
            throw new Error('Preview features response does not match version 1 of the public contract');

          if (typeof window !== 'undefined')
            window.__chPreviewFeaturesCache = data.previews;

          setPreviews(data.previews);
          setError(null);
        }
        catch (fetchError)
        {
          if (fetchError.name !== 'AbortError')
            console.error('Unable to load preview features:', fetchError);

          setError('Unable to load the current preview features. Please refresh the page and try again.');
        }
        finally
        {
          clearTimeout(timeoutId);
          setLoading(false);
        }
      }

      fetchPreviews();

      return () =>
      {
        clearTimeout(timeoutId);
        controller.abort();
      };
    }, []);

    return { previews, loading, error };
  }

  const { previews, loading, error } = usePreviewFeatures();

  if (loading)
    return <p role="status">Loading preview features…</p>;

  if (error)
    return <p role="alert">{error}</p>;

  return (
    <div className="my-6">
      <div className="overflow-x-auto rounded-md border border-gray-200 bg-white dark:border-gray-700 dark:bg-[#161616]">
        <table className="m-0 w-full table-fixed border-collapse text-left md:min-w-[760px]">
          <thead className="bg-gray-100 dark:bg-[#1f1f1f]">
            <tr>
              <th scope="col" className="border-b border-gray-200 px-5 py-3 text-sm font-semibold text-gray-900 dark:border-gray-700 dark:text-white md:w-[62%]">Feature</th>
              <th scope="col" className="hidden border-b border-gray-200 px-5 py-3 text-sm font-semibold text-gray-900 dark:border-gray-700 dark:text-white md:table-cell md:w-[18%]">Status</th>
              <th scope="col" className="hidden border-b border-gray-200 px-5 py-3 text-sm font-semibold text-gray-900 dark:border-gray-700 dark:text-white md:table-cell md:w-[20%]">Action</th>
            </tr>
          </thead>
          <tbody>
            {previews.map((preview) => (
              <tr key={preview.id} className="border-b border-gray-200 last:border-b-0 dark:border-gray-700">
                <td className="px-5 py-4 align-top">
                  <div>
                    <div className="flex flex-wrap items-center gap-x-2 gap-y-1">
                      <div className="text-base font-semibold text-gray-950 dark:text-white">{preview.name}</div>
                      <a
                        href={getConsoleUrl(preview.id)}
                        className="inline-block no-underline hover:no-underline md:hidden"
                        aria-label={`${getPreviewTypeLabel(preview.type)}: ${getActionLabel(preview.type)}`}
                      >
                        <span className={preview.type === 'public-preview' ? 'publicPreviewBadge whitespace-nowrap' : 'privatePreviewBadge whitespace-nowrap'}>
                          {getPreviewTypeLabel(preview.type)}
                        </span>
                      </a>
                    </div>
                    <p className="mt-1 mb-0 max-w-4xl text-sm leading-6 text-gray-600 dark:text-gray-300">{preview.description}</p>
                    <div className="mt-2 flex flex-wrap items-center gap-x-2 gap-y-1">
                      {preview.docsUrl && (
                        <a href={preview.docsUrl} className="inline-block text-sm font-medium no-underline">
                          Learn more
                        </a>
                      )}
                      {preview.docsUrl && <span className="text-sm text-gray-400 dark:text-gray-500 md:hidden" aria-hidden="true">|</span>}
                      <a
                        href={getConsoleUrl(preview.id)}
                        className="inline-block text-sm font-medium no-underline md:hidden"
                      >
                        {getActionLabel(preview.type)} <span aria-hidden="true">↗</span>
                      </a>
                    </div>
                  </div>
                </td>
                <td className="hidden px-5 py-4 align-top md:table-cell">
                  <a
                    href={getConsoleUrl(preview.id)}
                    className="inline-block no-underline hover:no-underline"
                    aria-label={`${getPreviewTypeLabel(preview.type)}: ${getActionLabel(preview.type)}`}
                  >
                    <span className={preview.type === 'public-preview' ? 'publicPreviewBadge whitespace-nowrap' : 'privatePreviewBadge whitespace-nowrap'}>
                      {getPreviewTypeLabel(preview.type)}
                    </span>
                  </a>
                </td>
                <td className="hidden px-5 py-4 align-top md:table-cell">
                  <a
                    href={getConsoleUrl(preview.id)}
                    className="inline-block text-sm font-medium no-underline"
                  >
                    {getActionLabel(preview.type)} <span aria-hidden="true">↗</span>
                  </a>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};
