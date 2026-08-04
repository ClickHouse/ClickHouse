import { galaxyOnClick } from '/snippets/lib/galaxy.jsx';

export const BetaBadge = ({ link, galaxyTrack, galaxyEvent }) => {
    if (link) {
        return (
            <a
                href={link}
                target="_blank"
                rel="noopener noreferrer"
                className="betaBadge"
                onClick={galaxyTrack && galaxyEvent ? galaxyOnClick(galaxyEvent) : undefined}
            >
                <span>ベータ</span>
            </a>
        )
    }

    return (
        <a
            href="/ja/reference/settings/beta-and-experimental-features#beta-features"
            className="betaBadge"
        >
            <span>ベータ機能です</span>
        </a>
    )
};